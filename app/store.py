"""SQLite persistence: raw events, branch corrections and lock types.

Branch model
------------
Branch 0 is the pristine import. Every correction (event edit/insert/delete
or lock type declaration) creates a child branch that *shares* unchanged
events with its parent. All parallel, legally valid reconstructions are
kept; nothing is overwritten.

Each branch row stores an effective event set computed at creation time
(denormalized into branch_events) so reads stay simple and branches
remain stable even if a sibling is later extended.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .events import Event, EventError, normalize_event

SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER,
    name TEXT NOT NULL,
    note TEXT DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts REAL NOT NULL,
    type TEXT NOT NULL,
    thread TEXT,
    lock TEXT,
    mode TEXT,
    cond TEXT,
    stack TEXT,
    gen INTEGER NOT NULL DEFAULT 0,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS branch_events (
    branch_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    PRIMARY KEY (branch_id, event_id)
);
CREATE TABLE IF NOT EXISTS branch_lock_types (
    branch_id INTEGER NOT NULL,
    lock_key TEXT NOT NULL,
    lock_type TEXT NOT NULL,
    PRIMARY KEY (branch_id, lock_key)
);
CREATE TABLE IF NOT EXISTS branch_corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    parent_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def lock_key(lock: str, gen: int) -> str:
    return "g%d:%s" % (gen, lock)


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        row = self.conn.execute("SELECT COUNT(*) c FROM branches").fetchone()
        if row["c"] == 0:
            self._create_branch_row(None, "原始轨迹", "pristine import")

    # ------------------------------------------------------------- branches
    def _create_branch_row(self, parent_id: Optional[int], name: str,
                           note: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO branches(parent_id, name, note, created_at) "
            "VALUES (?,?,?,?)",
            (parent_id, name, note,
             datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
        return int(cur.lastrowid)

    def list_branches(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM branches ORDER BY id").fetchall()
        result = []
        for row in rows:
            result.append({
                "id": row["id"], "parent_id": row["parent_id"],
                "name": row["name"], "note": row["note"],
                "created_at": row["created_at"],
                "event_count": self.conn.execute(
                    "SELECT COUNT(*) c FROM branch_events "
                    "WHERE branch_id=?", (row["id"],)).fetchone()["c"],
            })
        return result

    # --------------------------------------------------------------- events
    def add_events(self, raw_list: List[Dict[str, Any]],
                   branch_id: int = 0) -> int:
        """Append raw events (arrival order) and expose them on a branch.

        Defaults to the pristine branch. Existing branch event sets are
        untouched: corrections stay reproducible.
        """
        count = 0
        next_seq = self.conn.execute(
            "SELECT COALESCE(MAX(seq)+1,0) s FROM raw_events").fetchone()["s"]
        for raw in raw_list:
            payload = dict(raw)
            if "seq" not in payload:
                payload["seq"] = next_seq
                next_seq += 1
            ev = normalize_event(payload, int(payload["seq"]))
            cur = self.conn.execute(
                "INSERT INTO raw_events(seq, ts, type, thread, lock, mode,"
                " cond, stack, gen, payload) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ev.seq, ev.ts, ev.type, ev.thread, ev.lock, ev.mode,
                 ev.cond, json.dumps(ev.stack or []), ev.gen,
                 json.dumps(payload, ensure_ascii=False)))
            eid = int(cur.lastrowid)
            self.conn.execute(
                "INSERT OR IGNORE INTO branch_events(branch_id, event_id) "
                "VALUES (?,?)", (branch_id, eid))
            count += 1
        self.conn.commit()
        return count

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        return Event(
            id=row["id"], seq=row["seq"], ts=row["ts"], type=row["type"],
            thread=row["thread"], lock=row["lock"], mode=row["mode"],
            cond=row["cond"],
            stack=json.loads(row["stack"] or "[]"),
            gen=row["gen"])

    def get_events(self, branch_id: int) -> List[Event]:
        rows = self.conn.execute(
            "SELECT r.* FROM raw_events r JOIN branch_events b "
            "ON b.event_id = r.id WHERE b.branch_id=? ORDER BY r.seq",
            (branch_id,)).fetchall()
        return [self._row_to_event(r) for r in rows]

    def get_event(self, event_id: int) -> Event:
        row = self.conn.execute(
            "SELECT * FROM raw_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise KeyError("event %s not found" % event_id)
        return self._row_to_event(row)

    # ----------------------------------------------------------- lock types
    def set_lock_type(self, branch_id: int, lock: str, gen: int,
                      lock_type: str) -> None:
        from .events import LOCK_TYPES
        if lock_type not in LOCK_TYPES:
            raise EventError("invalid lock type %r" % lock_type)
        self.conn.execute(
            "INSERT INTO branch_lock_types(branch_id, lock_key, lock_type) "
            "VALUES (?,?,?) ON CONFLICT(branch_id, lock_key) "
            "DO UPDATE SET lock_type=excluded.lock_type",
            (branch_id, lock_key(lock, gen), lock_type))
        self.conn.commit()

    def get_lock_types(self, branch_id: int) -> Dict[str, str]:
        # inherit parent declarations, child overrides for same key
        chain = []
        current = branch_id
        seen = set()
        while current is not None and current not in seen:
            seen.add(current)
            chain.append(current)
            row = self.conn.execute(
                "SELECT parent_id FROM branches WHERE id=?",
                (current,)).fetchone()
            current = row["parent_id"] if row else None
        result: Dict[str, str] = {}
        for bid in reversed(chain):
            rows = self.conn.execute(
                "SELECT lock_key, lock_type FROM branch_lock_types "
                "WHERE branch_id=?", (bid,)).fetchall()
            for row in rows:
                result[row["lock_key"]] = row["lock_type"]
        return result

    # --------------------------------------------------------- corrections
    def create_correction_branch(self, parent_id: int, name: str,
                                 note: str) -> int:
        new_id = self._create_branch_row(parent_id, name, note)
        self.conn.execute(
            "INSERT INTO branch_events(branch_id, event_id) "
            "SELECT ?, event_id FROM branch_events WHERE branch_id=?",
            (new_id, parent_id))
        self.conn.execute(
            "INSERT INTO branch_lock_types(branch_id, lock_key, lock_type) "
            "SELECT ?, lock_key, lock_type FROM branch_lock_types "
            "WHERE branch_id=?", (new_id, parent_id))
        self.conn.commit()
        return new_id

    def correct_event(self, parent_id: int, kind: str,
                      payload: Dict[str, Any], name: str,
                      note: str = "") -> Tuple[int, Dict[str, Any]]:
        """kind: edit | insert | delete. Returns (new_branch_id, detail)."""
        new_id = self.create_correction_branch(parent_id, name, note)
        detail: Dict[str, Any] = {"kind": kind}

        if kind == "edit":
            event_id = int(payload["event_id"])
            original = self.get_event(event_id)
            merged = json.loads(
                self.conn.execute(
                    "SELECT payload FROM raw_events WHERE id=?",
                    (event_id,)).fetchone()["payload"])
            merged.update({k: v for k, v in payload.items()
                           if k != "event_id"})
            corrected = normalize_event(merged, original.seq)
            # clone into a new raw row so the parent branch stays immutable
            cur = self.conn.execute(
                "INSERT INTO raw_events(seq, ts, type, thread, lock, mode,"
                " cond, stack, gen, payload) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (corrected.seq, corrected.ts, corrected.type,
                 corrected.thread, corrected.lock, corrected.mode,
                 corrected.cond, json.dumps(corrected.stack or []),
                 corrected.gen,
                 json.dumps(merged, ensure_ascii=False)))
            new_eid = int(cur.lastrowid)
            self.conn.execute(
                "DELETE FROM branch_events WHERE branch_id=? AND event_id=?",
                (new_id, event_id))
            self.conn.execute(
                "INSERT INTO branch_events(branch_id, event_id) "
                "VALUES (?,?)", (new_id, new_eid))
            detail["event_id"] = event_id
            detail["new_event_id"] = new_eid
            detail["before"] = original.to_dict()
            detail["after"] = corrected.to_dict()

        elif kind == "insert":
            events = payload.get("events") or [payload.get("event") or payload]
            inserted = []
            for raw in events:
                raw = dict(raw)
                raw.setdefault("seq", self.conn.execute(
                    "SELECT COALESCE(MAX(seq)+1,0) s FROM raw_events")
                    .fetchone()["s"])
                ev = normalize_event(raw, int(raw["seq"]))
                cur = self.conn.execute(
                    "INSERT INTO raw_events(seq, ts, type, thread, lock,"
                    " mode, cond, stack, gen, payload) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (ev.seq, ev.ts, ev.type, ev.thread, ev.lock, ev.mode,
                     ev.cond, json.dumps(ev.stack or []), ev.gen,
                     json.dumps(raw, ensure_ascii=False)))
                eid = int(cur.lastrowid)
                self.conn.execute(
                    "INSERT INTO branch_events(branch_id, event_id) "
                    "VALUES (?,?)", (new_id, eid))
                inserted.append(eid)
            detail["inserted_ids"] = inserted

        elif kind == "delete":
            event_id = int(payload["event_id"])
            original = self.get_event(event_id)
            self.conn.execute(
                "DELETE FROM branch_events WHERE branch_id=? AND event_id=?",
                (new_id, event_id))
            detail["event_id"] = event_id
            detail["before"] = original.to_dict()
        else:
            raise EventError("unknown correction kind %r" % kind)

        self.conn.execute(
            "INSERT INTO branch_corrections(branch_id, parent_id, kind,"
            " detail, created_at) VALUES (?,?,?,?,?)",
            (new_id, parent_id, kind,
             json.dumps(detail, ensure_ascii=False),
             datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
        return new_id, detail

    def declare_lock_type(self, parent_id: int, lock: str, gen: int,
                          lock_type: str, name: str,
                          note: str = "") -> Tuple[int, Dict[str, Any]]:
        new_id = self.create_correction_branch(parent_id, name, note)
        self.set_lock_type(new_id, lock, gen, lock_type)
        detail = {"kind": "declare_type", "lock": lock, "gen": gen,
                  "lock_type": lock_type}
        self.conn.execute(
            "INSERT INTO branch_corrections(branch_id, parent_id, kind,"
            " detail, created_at) VALUES (?,?,?,?,?)",
            (new_id, parent_id, "declare_type",
             json.dumps(detail, ensure_ascii=False),
             datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
        return new_id, detail

    def _replace_event(self, event_id: int, ev: Event) -> None:
        self.conn.execute(
            "UPDATE raw_events SET seq=?, ts=?, type=?, thread=?, lock=?,"
            " mode=?, cond=?, stack=?, gen=? WHERE id=?",
            (ev.seq, ev.ts, ev.type, ev.thread, ev.lock, ev.mode, ev.cond,
             json.dumps(ev.stack or []), ev.gen, event_id))
        self.conn.commit()

    def corrections(self, branch_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM branch_corrections WHERE branch_id=? "
            "ORDER BY id", (branch_id,)).fetchall()
        return [{"id": r["id"], "parent_id": r["parent_id"],
                 "kind": r["kind"], "detail": json.loads(r["detail"])}
                for r in rows]

    def compare(self, branch_a: int, branch_b: int
                ) -> Dict[str, Any]:
        events_a = {e.id: e for e in self.get_events(branch_a)}
        events_b = {e.id: e for e in self.get_events(branch_b)}
        ids_a, ids_b = set(events_a), set(events_b)
        changed = []
        for eid in sorted(ids_a & ids_b):
            a, b = events_a[eid].to_dict(), events_b[eid].to_dict()
            if a != b:
                changed.append({"event_id": eid, "a": a, "b": b})
        types_a = self.get_lock_types(branch_a)
        types_b = self.get_lock_types(branch_b)
        type_diffs = []
        for key in sorted(set(types_a) | set(types_b)):
            if types_a.get(key) != types_b.get(key):
                type_diffs.append({"lock_key": key,
                                   "a": types_a.get(key),
                                   "b": types_b.get(key)})
        return {
            "a": branch_a, "b": branch_b,
            "only_in_a": sorted(ids_a - ids_b),
            "only_in_b": sorted(ids_b - ids_a),
            "changed": changed,
            "lock_type_diffs": type_diffs,
        }
