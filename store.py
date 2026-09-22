from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from engine import Event, ResourceKey, declaration_key, event_from_dict


SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    branch_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    raw TEXT NOT NULL,
    PRIMARY KEY (branch_id, event_id)
);
CREATE TABLE IF NOT EXISTS declarations (
    branch_id TEXT NOT NULL,
    process_id TEXT NOT NULL,
    process_generation TEXT NOT NULL,
    lock_name TEXT NOT NULL,
    lock_type TEXT NOT NULL,
    PRIMARY KEY (branch_id, process_id, process_generation, lock_name)
);
"""


class Store:
    def __init__(self, path: str = "locklab.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def create_branch(self, name: str, parent_id: Optional[str] = None) -> str:
        branch_id = "br_" + uuid.uuid4().hex[:10]
        self.conn.execute(
            "INSERT INTO branches (id, name, parent_id, created_at) VALUES (?, ?, ?, ?)",
            (branch_id, name, parent_id, time.time()),
        )
        self.conn.commit()
        return branch_id

    def list_branches(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, name, parent_id, created_at FROM branches ORDER BY created_at"
        ).fetchall()
        return [dict(row) for row in rows]

    def branch_exists(self, branch_id: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM branches WHERE id = ?", (branch_id,)).fetchone()
        return row is not None

    def next_seq(self, branch_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM events WHERE branch_id = ?", (branch_id,)
        ).fetchone()
        return int(row["max_seq"]) + 1

    def add_events(self, branch_id: str, raws: List[Dict[str, Any]]) -> List[Event]:
        seq = self.next_seq(branch_id)
        events: List[Event] = []
        for raw in raws:
            raw = dict(raw)
            raw["seq"] = seq
            event = event_from_dict(raw, seq_override=seq)
            self.conn.execute(
                "INSERT OR REPLACE INTO events (branch_id, event_id, seq, raw) VALUES (?, ?, ?, ?)",
                (branch_id, event.id, seq, json.dumps(raw, ensure_ascii=False)),
            )
            events.append(event)
            seq += 1
        self.conn.commit()
        return events

    def delete_event(self, branch_id: str, event_id: str) -> bool:
        cursor = self.conn.execute(
            "DELETE FROM events WHERE branch_id = ? AND event_id = ?", (branch_id, event_id)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def update_event(self, branch_id: str, event_id: str, raw: Dict[str, Any]) -> bool:
        cursor = self.conn.execute(
            "UPDATE events SET raw = ? WHERE branch_id = ? AND event_id = ?",
            (json.dumps(raw, ensure_ascii=False), branch_id, event_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def list_events(self, branch_id: str) -> List[Event]:
        rows = self.conn.execute(
            "SELECT raw FROM events WHERE branch_id = ? ORDER BY seq", (branch_id,)
        ).fetchall()
        return [event_from_dict(json.loads(row["raw"])) for row in rows]

    def set_declaration(self, branch_id: str, declaration: Dict[str, Any]) -> Tuple[ResourceKey, str]:
        key = declaration_key(declaration)
        lock_type = str(declaration.get("lock_type", declaration.get("type", "")) or "")
        self.conn.execute(
            "INSERT OR REPLACE INTO declarations "
            "(branch_id, process_id, process_generation, lock_name, lock_type) VALUES (?, ?, ?, ?, ?)",
            (branch_id, key.process_id, key.process_generation, key.name, lock_type),
        )
        self.conn.commit()
        return key, lock_type

    def list_declarations(self, branch_id: str) -> Dict[ResourceKey, str]:
        rows = self.conn.execute(
            "SELECT process_id, process_generation, lock_name, lock_type FROM declarations WHERE branch_id = ?",
            (branch_id,),
        ).fetchall()
        return {
            ResourceKey("lock", row["process_id"], row["process_generation"], row["lock_name"]): row["lock_type"]
            for row in rows
        }

    def clone_branch(self, source_id: str, name: str) -> str:
        new_id = self.create_branch(name, parent_id=source_id)
        self.conn.execute(
            "INSERT INTO events (branch_id, event_id, seq, raw) "
            "SELECT ?, event_id, seq, raw FROM events WHERE branch_id = ?",
            (new_id, source_id),
        )
        self.conn.execute(
            "INSERT INTO declarations (branch_id, process_id, process_generation, lock_name, lock_type) "
            "SELECT ?, process_id, process_generation, lock_name, lock_type FROM declarations WHERE branch_id = ?",
            (new_id, source_id),
        )
        self.conn.commit()
        return new_id
