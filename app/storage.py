"""SQLite-backed storage with correction branches.

Only the ``base`` branch accepts raw ingested events (in arrival order).
Other branches materialize the events of their parent chain and apply a
cumulative list of corrections:

* ``update``   - patch fields of an existing event
* ``insert``   - add a synthetic event
* ``delete``   - remove an event
* ``declare``  - assert a lock type (mutex/recursive/rwlock)
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    Event, EVENT_KINDS, LOCK_TYPES, MUTEX,
)
from .engine import ReplayEngine, ReplayResult, StreamEngine

SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id TEXT PRIMARY KEY,
    parent TEXT,
    description TEXT,
    created_at REAL,
    corrections TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS declarations (
    branch_id TEXT NOT NULL,
    gen INTEGER NOT NULL,
    lock TEXT NOT NULL,
    ltype TEXT NOT NULL,
    PRIMARY KEY (branch_id, gen, lock)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gen INTEGER NOT NULL,
    ts REAL NOT NULL,
    arrival_seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    thread TEXT NOT NULL,
    lock TEXT,
    mode TEXT,
    cond TEXT,
    timeout INTEGER DEFAULT 0,
    stack TEXT,
    data TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_order ON events(gen, ts, arrival_seq);
"""

_EVENT_COLS = [
    "gen", "ts", "arrival_seq", "kind", "thread", "lock", "mode",
    "cond", "timeout", "stack", "data",
]


class LabStore:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        self._stream = StreamEngine()
        self._replay_cache: Dict[str, ReplayResult] = {}
        self._ensure_base()

    def _ensure_base(self):
        row = self._conn.execute(
            "SELECT id FROM branches WHERE id='base'").fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO branches(id, parent, description, created_at) "
                "VALUES('base', NULL, ?, ?)",
                ("原始事件流（按采集顺序）", time.time()))
            self._conn.commit()

    # -- ingestion ---------------------------------------------------------
    def add_events(self, payloads: List[dict]) -> List[int]:
        with self._lock:
            ids: List[int] = []
            events: List[Event] = []
            for p in payloads:
                ev = Event.from_dict(p)
                if ev.kind not in EVENT_KINDS:
                    raise ValueError(f"unknown event kind: {ev.kind!r}")
                cur = self._conn.execute(
                    "INSERT INTO events(" + ",".join(_EVENT_COLS) + ") "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (ev.gen, float(ev.ts), int(time.time() * 1_000_000),
                     ev.kind, ev.thread, ev.lock, ev.mode, ev.cond,
                     1 if ev.timeout else 0,
                     json.dumps(ev.stack, ensure_ascii=False),
                     json.dumps(ev.data, ensure_ascii=False)))
                ev.id = cur.lastrowid
                ids.append(ev.id)
                events.append(ev)
            self._conn.commit()
            self._stream.append(events)
            self._replay_cache.pop("base", None)
            return ids

    def raw_events_base(self) -> List[dict]:
        """Events in arrival (ingestion) order - the audit trail."""
        rows = self._conn.execute(
            "SELECT * FROM events ORDER BY id").fetchall()
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(r: sqlite3.Row) -> dict:
        return {
            "id": r["id"], "gen": r["gen"], "ts": r["ts"],
            "arrival_seq": r["arrival_seq"], "kind": r["kind"],
            "thread": r["thread"], "lock": r["lock"], "mode": r["mode"],
            "cond": r["cond"], "timeout": bool(r["timeout"]),
            "stack": json.loads(r["stack"]) if r["stack"] else None,
            "data": json.loads(r["data"]) if r["data"] else {},
        }

    # -- branches ----------------------------------------------------------
    def list_branches(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM branches ORDER BY created_at").fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r["id"], "parent": r["parent"],
                "description": r["description"],
                "created_at": r["created_at"],
                "corrections": json.loads(r["corrections"]),
            })
        return out

    def create_branch(self, branch_id: str, parent: str,
                      description: str = "") -> dict:
        with self._lock:
            if self._conn.execute("SELECT 1 FROM branches WHERE id=?",
                                  (branch_id,)).fetchone():
                raise ValueError(f"branch {branch_id!r} exists")
            if not self._conn.execute("SELECT 1 FROM branches WHERE id=?",
                                      (parent,)).fetchone():
                raise ValueError(f"parent {parent!r} missing")
            self._conn.execute(
                "INSERT INTO branches(id, parent, description, created_at, "
                "corrections) VALUES(?,?,?,?, '[]')",
                (branch_id, parent, description, time.time()))
            self._conn.commit()
            return {"id": branch_id, "parent": parent,
                    "description": description, "corrections": []}

    def add_correction(self, branch_id: str, correction: dict) -> dict:
        with self._lock:
            if branch_id == "base":
                raise ValueError("base branch cannot be corrected; fork it")
            row = self._conn.execute(
                "SELECT corrections FROM branches WHERE id=?",
                (branch_id,)).fetchone()
            if row is None:
                raise ValueError(f"branch {branch_id!r} missing")
            correction = self._validate_correction(dict(correction))
            items = json.loads(row["corrections"])
            correction["at"] = time.time()
            items.append(correction)
            self._conn.execute("UPDATE branches SET corrections=? WHERE id=?",
                               (json.dumps(items, ensure_ascii=False),
                                branch_id))
            if correction["op"] == "declare":
                self._conn.execute(
                    "INSERT OR REPLACE INTO declarations"
                    "(branch_id, gen, lock, ltype) VALUES(?,?,?,?)",
                    (branch_id, correction["gen"], correction["lock"],
                     correction["type"]))
            self._conn.commit()
            self._replay_cache.pop(branch_id, None)
            return correction

    @staticmethod
    def _validate_correction(c: dict) -> dict:
        op = c.get("op")
        if op in ("update", "delete"):
            if not isinstance(c.get("id"), int):
                raise ValueError("correction requires integer event id")
            if op == "update":
                patch = c.get("patch") or {}
                allowed = {"ts", "kind", "thread", "lock", "mode", "cond",
                           "timeout", "stack", "data", "gen"}
                bad = set(patch) - allowed
                if bad:
                    raise ValueError(f"patch fields not allowed: {sorted(bad)}")
                if "kind" in patch and patch["kind"] not in EVENT_KINDS:
                    raise ValueError("unknown kind in patch")
                c["patch"] = patch
        elif op == "insert":
            ev = c.get("event")
            if not isinstance(ev, dict) or "kind" not in ev:
                raise ValueError("insert requires event object")
            if ev["kind"] not in EVENT_KINDS:
                raise ValueError("unknown inserted event kind")
        elif op == "declare":
            if c.get("type") not in LOCK_TYPES:
                raise ValueError(f"type must be one of {sorted(LOCK_TYPES)}")
            if "lock" not in c or "gen" not in c:
                raise ValueError("declare requires gen and lock")
        else:
            raise ValueError(f"unknown correction op {op!r}")
        c.setdefault("note", "")
        return c

    # -- branch materialization -------------------------------------------
    def _branch_chain(self, branch_id: str) -> List[str]:
        chain: List[str] = []
        cur: Optional[str] = branch_id
        seen = set()
        while cur is not None:
            if cur in seen:
                raise ValueError("branch parent cycle")
            seen.add(cur)
            chain.append(cur)
            row = self._conn.execute(
                "SELECT parent FROM branches WHERE id=?", (cur,)).fetchone()
            cur = row["parent"] if row else None
        chain.reverse()
        return chain

    def _declarations_for(self, chain: List[str]) -> Dict[str, str]:
        decls: Dict[str, str] = {}
        for bid in chain:
            rows = self._conn.execute(
                "SELECT gen, lock, ltype FROM declarations WHERE branch_id=?",
                (bid,)).fetchall()
            for r in rows:
                decls[f"{r['gen']}:{r['lock']}"] = r["ltype"]
        return decls

    def branch_events(self, branch_id: str) -> List[Event]:
        """Effective ordered event list for a branch (logical order)."""
        with self._lock:
            if branch_id == "base":
                rows = self._conn.execute(
                    "SELECT * FROM events ORDER BY gen, ts, arrival_seq"
                ).fetchall()
                events = []
                for i, r in enumerate(rows):
                    d = self._row_to_dict(r)
                    events.append(Event.from_dict(d, seq=i, id_=r["id"]))
                return events
            chain = self._branch_chain(branch_id)
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY gen, ts, arrival_seq"
            ).fetchall()
            events: Dict[int, Event] = {}
            for i, r in enumerate(rows):
                d = self._row_to_dict(r)
                events[r["id"]] = Event.from_dict(d, seq=i, id_=r["id"])
            synth_seq = -1
            for bid in chain:
                row = self._conn.execute(
                    "SELECT corrections FROM branches WHERE id=?",
                    (bid,)).fetchone()
                for cor in json.loads(row["corrections"]):
                    if cor["op"] == "delete":
                        events.pop(cor["id"], None)
                    elif cor["op"] == "update":
                        ev = events.get(cor["id"])
                        if ev is None:
                            continue
                        for k, v in cor["patch"].items():
                            setattr(ev, k, v)
                    elif cor["op"] == "insert":
                        spec = cor["event"]
                        synth_seq -= 1
                        ev = Event.from_dict(spec, seq=synth_seq,
                                             id_=cor.get("synthetic_id",
                                                         synth_seq))
                        events[ev.id] = ev
            return sorted(events.values(), key=lambda e: e.sort_key())

    def replay(self, branch_id: str = "base",
               force: bool = False) -> ReplayResult:
        with self._lock:
            if not force and branch_id in self._replay_cache:
                return self._replay_cache[branch_id]
            if branch_id == "base":
                result = self._stream.full_result()
            else:
                chain = self._branch_chain(branch_id)
                decls = self._declarations_for(chain)
                events = self.branch_events(branch_id)
                result = ReplayEngine(lock_types=decls).run(events)
            self._replay_cache[branch_id] = result
            return result

    def branch_info(self, branch_id: str) -> dict:
        row = self._conn.execute("SELECT * FROM branches WHERE id=?",
                                 (branch_id,)).fetchone()
        if row is None:
            raise ValueError(f"branch {branch_id!r} missing")
        chain = self._branch_chain(branch_id)
        return {
            "id": row["id"], "parent": row["parent"],
            "description": row["description"],
            "created_at": row["created_at"],
            "corrections": json.loads(row["corrections"]),
            "chain": chain,
            "declarations": self._declarations_for(chain),
        }

    def lock_declarations(self) -> Dict[str, str]:
        return self._declarations_for(["base"] + [
            r["id"] for r in self._conn.execute(
                "SELECT id FROM branches WHERE id != 'base'").fetchall()])

    # -- streaming stats ---------------------------------------------------
    def stream_stats(self) -> dict:
        s = self._stream
        last = s.stats_history[-1] if s.stats_history else None
        return {
            "total_events": len(s.events),
            "total_frames": len(s._ordered),
            "checkpoints": sorted(s._checkpoints),
            "last_append": {
                "inserted": last.inserted, "late": last.late,
                "replay_from_frame": last.replay_from_frame,
                "frames_recomputed": last.frames_recomputed,
                "total_frames": last.total_frames,
            } if last else None,
        }

    def close(self):
        self._conn.close()
