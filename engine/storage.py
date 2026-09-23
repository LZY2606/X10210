"""SQLite persistence for runs, raw events, branches and corrections."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id    TEXT PRIMARY KEY,
    name      TEXT,
    created_at REAL,
    seed_seq  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    run_id  TEXT NOT NULL,
    idx     INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (run_id, idx)
);
CREATE TABLE IF NOT EXISTS branches (
    run_id      TEXT NOT NULL,
    branch_id   TEXT NOT NULL,
    parent_id   TEXT,
    label       TEXT,
    lock_types  TEXT NOT NULL DEFAULT '{}',
    created_at  REAL,
    PRIMARY KEY (run_id, branch_id)
);
CREATE TABLE IF NOT EXISTS corrections (
    run_id    TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    idx       INTEGER,
    event_id  TEXT,
    action    TEXT NOT NULL,
    patch     TEXT NOT NULL,
    note      TEXT
);
"""


class Store:
    def __init__(self, path: str = "locklab.db"):
        self.path = path
        first = not os.path.exists(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        if first:
            self.conn.executescript(SCHEMA)
            self.conn.commit()
        else:
            self.conn.executescript(SCHEMA)

    # ---- runs ----
    def create_run(self, run_id: str, name: str, created_at: float) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO runs(run_id, name, created_at, seed_seq) VALUES(?,?,?,0)",
                (run_id, name, created_at),
            )
            self.conn.commit()

    def list_runs(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM runs ORDER BY created_at").fetchall()
            return [dict(r) for r in rows]

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    # ---- raw events ----
    def append_events(self, run_id: str, raws: List[Dict[str, Any]]) -> int:
        with self._lock:
            row = self.conn.execute("SELECT seed_seq FROM runs WHERE run_id=?", (run_id,)).fetchone()
            idx = row["seed_seq"] if row else 0
            for raw in raws:
                self.conn.execute(
                    "INSERT OR REPLACE INTO events(run_id, idx, payload) VALUES(?,?,?)",
                    (run_id, idx, json.dumps(raw, ensure_ascii=False)),
                )
                idx += 1
            self.conn.execute("UPDATE runs SET seed_seq=? WHERE run_id=?", (idx, run_id))
            self.conn.commit()
            return idx

    def raw_events(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT payload FROM events WHERE run_id=? ORDER BY idx", (run_id,)
            ).fetchall()
            return [json.loads(r["payload"]) for r in rows]

    # ---- branches ----
    def upsert_branch(
        self, run_id: str, branch_id: str, parent_id: Optional[str], label: str,
        lock_types: Dict[str, str], created_at: float,
    ) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO branches(run_id, branch_id, parent_id, label, lock_types, created_at)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(run_id, branch_id) DO UPDATE SET
                     parent_id=excluded.parent_id, label=excluded.label,
                     lock_types=excluded.lock_types""",
                (run_id, branch_id, parent_id, label,
                 json.dumps(lock_types, ensure_ascii=False), created_at),
            )
            self.conn.commit()

    def list_branches(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM branches WHERE run_id=? ORDER BY created_at", (run_id,)
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["lock_types"] = json.loads(d["lock_types"])
                out.append(d)
            return out

    def get_branch(self, run_id: str, branch_id: str) -> Optional[Dict[str, Any]]:
        for b in self.list_branches(run_id):
            if b["branch_id"] == branch_id:
                return b
        return None

    def add_correction(self, run_id: str, branch_id: str, corr: Dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO corrections(run_id, branch_id, idx, event_id, action, patch, note)
                   VALUES(?,?,?,?,?,?,?)""",
                (run_id, branch_id, corr.get("idx"), corr.get("event_id"),
                 corr.get("action", "update"),
                 json.dumps(corr.get("patch", {}), ensure_ascii=False), corr.get("note")),
            )
            self.conn.commit()

    def corrections(self, run_id: str, branch_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM corrections WHERE run_id=? AND branch_id=? ORDER BY rowid",
                (run_id, branch_id),
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["patch"] = json.loads(d["patch"])
                out.append(d)
        # include corrections inherited from parent chain
        branch = self.get_branch(run_id, branch_id)
        if branch and branch.get("parent_id"):
            parent_corr = self.corrections(run_id, branch["parent_id"])
            own_ids = {(c["idx"], c["event_id"]) for c in out}
            for c in parent_corr:
                if (c["idx"], c["event_id"]) not in own_ids:
                    out.append(dict(c, inherited_from=branch["parent_id"]))
        return out

    def close(self) -> None:
        with self._lock:
            self.conn.close()
