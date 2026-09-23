"""SQLite persistence for base events, branches and corrections."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import Event, event_from_dict

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS branch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER,
    note TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS event_row (
    seq INTEGER PRIMARY KEY,
    ts REAL NOT NULL,
    arrival REAL NOT NULL,
    kind TEXT NOT NULL,
    thread TEXT NOT NULL,
    gen INTEGER NOT NULL,
    lock_addr TEXT,
    cond_addr TEXT,
    mode TEXT NOT NULL,
    req_id TEXT,
    stack TEXT NOT NULL,
    note TEXT
);
CREATE TABLE IF NOT EXISTS correction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    op TEXT NOT NULL,
    target_seq INTEGER,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""


class Store:
    def __init__(self, path: str = "dissection.db"):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True) if Path(path).parent.parts else None
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._bootstrap()

    def _bootstrap(self) -> None:
        row = self.conn.execute("SELECT id FROM branch WHERE id = 0").fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO branch (id, name, parent_id, note, created_at) "
                "VALUES (0, 'main', NULL, 'baseline import', 0)"
            )
            self.conn.commit()
        row = self.get_meta("lock_types")
        if row is None:
            self.set_meta("lock_types", {})

    # ------------------------------------------------------------- meta
    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value, ensure_ascii=False)),
        )
        self.conn.commit()

    # ------------------------------------------------------------- events
    def replace_events(self, events: list[Event]) -> None:
        self.conn.execute("DELETE FROM event_row")
        self._insert_rows(events)
        self.conn.commit()

    def append_event(self, event: Event) -> None:
        self._insert_rows([event])
        self.conn.commit()

    def _insert_rows(self, events: list[Event]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO event_row "
            "(seq, ts, arrival, kind, thread, gen, lock_addr, cond_addr, mode, "
            " req_id, stack, note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    event.seq,
                    event.ts,
                    event.arrival,
                    event.kind,
                    event.thread,
                    event.gen,
                    event.lock,
                    event.cond,
                    event.mode,
                    event.req_id,
                    json.dumps(event.stack, ensure_ascii=False),
                    event.note,
                )
                for event in events
            ],
        )

    def load_events(self) -> list[Event]:
        rows = self.conn.execute("SELECT * FROM event_row ORDER BY seq").fetchall()
        return [self._row_to_event(row) for row in rows]

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        return Event(
            seq=row["seq"],
            ts=row["ts"],
            arrival=row["arrival"],
            kind=row["kind"],
            thread=row["thread"],
            gen=row["gen"],
            lock=row["lock_addr"],
            cond=row["cond_addr"],
            mode=row["mode"],
            req_id=row["req_id"],
            stack=json.loads(row["stack"] or "[]"),
            note=row["note"],
        )

    def next_seq(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(seq), -1) + 1 AS n FROM event_row").fetchone()
        return int(row["n"])

    # ------------------------------------------------------------- branches
    def list_branches(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM branch ORDER BY id").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["corrections"] = self.list_corrections(item["id"])
            result.append(item)
        return result

    def create_branch(self, name: str, parent_id: int, note: str | None) -> int:
        import time

        cursor = self.conn.execute(
            "INSERT INTO branch (name, parent_id, note, created_at) VALUES (?,?,?,?)",
            (name, parent_id, note, time.time()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def get_branch(self, branch_id: int) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM branch WHERE id = ?", (branch_id,)).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["corrections"] = self.list_corrections(branch_id)
        return item

    def add_correction(self, branch_id: int, op: str, payload: dict[str, Any], target_seq: int | None) -> int:
        import time

        cursor = self.conn.execute(
            "INSERT INTO correction (branch_id, op, target_seq, payload, created_at) "
            "VALUES (?,?,?,?,?)",
            (branch_id, op, target_seq, json.dumps(payload, ensure_ascii=False), time.time()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def list_corrections(self, branch_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM correction WHERE branch_id = ? ORDER BY id", (branch_id,)
        ).fetchall()
        return [
            {
                "id": row["id"],
                "op": row["op"],
                "target_seq": row["target_seq"],
                "payload": json.loads(row["payload"]),
            }
            for row in rows
        ]

    def chain(self, branch_id: int) -> list[dict[str, Any]]:
        chain: list[dict[str, Any]] = []
        current = self.get_branch(branch_id)
        while current is not None:
            chain.append(current)
            if current["parent_id"] is None:
                break
            current = self.get_branch(current["parent_id"])
        chain.reverse()
        return chain

    def materialize(self, branch_id: int) -> tuple[list[Event], dict[str, str]]:
        events = self.load_events()
        lock_types = dict(self.get_meta("lock_types", {}))
        for branch in self.chain(branch_id):
            events, lock_types = self._apply_corrections(
                events, lock_types, branch["corrections"]
            )
        return sorted(events, key=lambda event: event.key()), lock_types

    def _apply_corrections(
        self,
        events: list[Event],
        lock_types: dict[str, str],
        corrections: list[dict[str, Any]],
    ) -> tuple[list[Event], dict[str, str]]:
        by_seq = {event.seq: event for event in events}
        delete = set()
        next_seq = (max(by_seq) + 1) if by_seq else 0
        for correction in corrections:
            op = correction["op"]
            payload = correction["payload"]
            target = correction["target_seq"]
            if op == "delete_event":
                if target in by_seq:
                    delete.add(target)
            elif op == "edit_event":
                if target in by_seq:
                    data = by_seq[target].to_dict()
                    data.update(payload)
                    data["seq"] = target
                    by_seq[target] = event_from_dict(data, seq=target)
            elif op == "insert_event":
                data = dict(payload)
                data["seq"] = data.get("seq", next_seq)
                seq = int(data["seq"])
                by_seq[seq] = event_from_dict(data, seq=seq)
                next_seq = max(next_seq, seq + 1)
            elif op == "set_lock_type":
                key = payload.get("key")
                value = payload.get("type")
                if key and value:
                    lock_types[key] = value
        remaining = [event for seq, event in by_seq.items() if seq not in delete]
        return remaining, lock_types

    def close(self) -> None:
        self.conn.close()
