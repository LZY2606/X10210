from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

from .engine import LOCK_TYPES, analyze


DEFAULT_DB = os.environ.get("LOCKLAB_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), ".venv", "locklab.sqlite3"))


def connect(path: Optional[str] = None) -> sqlite3.Connection:
    resolved = path or DEFAULT_DB
    directory = os.path.dirname(resolved)
    if directory and resolved != ":memory:":
        os.makedirs(directory, exist_ok=True)
    connection = sqlite3.connect(resolved, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS branches (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            name TEXT NOT NULL,
            note TEXT DEFAULT '',
            created_at REAL NOT NULL,
            FOREIGN KEY(parent_id) REFERENCES branches(id)
        );
        CREATE TABLE IF NOT EXISTS events (
            branch_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            generation TEXT NOT NULL,
            timestamp REAL NOT NULL,
            arrival_seq INTEGER NOT NULL,
            kind TEXT NOT NULL,
            thread TEXT NOT NULL,
            lock_address TEXT,
            mode TEXT NOT NULL,
            request_id TEXT,
            cond TEXT,
            broadcast INTEGER NOT NULL DEFAULT 0,
            stack_json TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY(branch_id, event_id),
            FOREIGN KEY(branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS lock_types (
            branch_id TEXT NOT NULL,
            generation TEXT NOT NULL,
            lock_address TEXT NOT NULL,
            lock_type TEXT NOT NULL,
            PRIMARY KEY(branch_id, generation, lock_address),
            FOREIGN KEY(branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );
        """
    )
    connection.execute(
        "INSERT OR IGNORE INTO branches(id, parent_id, name, note, created_at) VALUES (?, NULL, ?, ?, ?)",
        ("main", "主干", "", time.time()),
    )
    connection.commit()


def event_payload(row: sqlite3.Row) -> Dict[str, Any]:
    payload = json.loads(row["payload_json"])
    payload.update(
        {
            "id": row["event_id"],
            "generation": row["generation"],
            "timestamp": row["timestamp"],
            "seq": row["arrival_seq"],
            "kind": row["kind"],
            "thread": row["thread"],
            "lock": row["lock_address"],
            "mode": row["mode"],
            "request_id": row["request_id"],
            "cond": row["cond"],
            "broadcast": bool(row["broadcast"]),
            "stack": json.loads(row["stack_json"]),
        }
    )
    return payload


def list_branches(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = connection.execute("SELECT * FROM branches ORDER BY created_at, id").fetchall()
    return [dict(row) for row in rows]


def get_branch(connection: sqlite3.Connection, branch_id: str) -> Dict[str, Any]:
    row = connection.execute("SELECT * FROM branches WHERE id = ?", (branch_id,)).fetchone()
    if row is None:
        raise KeyError(f"unknown branch: {branch_id}")
    return dict(row)


def load_events(connection: sqlite3.Connection, branch_id: str) -> List[Dict[str, Any]]:
    rows = connection.execute(
        "SELECT * FROM events WHERE branch_id = ? ORDER BY timestamp, arrival_seq, event_id",
        (branch_id,),
    ).fetchall()
    return [event_payload(row) for row in rows]


def load_lock_types(connection: sqlite3.Connection, branch_id: str) -> Dict[Tuple[str, str], str]:
    rows = connection.execute("SELECT * FROM lock_types WHERE branch_id = ?", (branch_id,)).fetchall()
    return {(row["generation"], row["lock_address"]): row["lock_type"] for row in rows}


def prepare_event(row: Dict[str, Any], fallback_seq: int) -> Dict[str, Any]:
    required = ("kind", "thread")
    for key in required:
        if not row.get(key):
            raise ValueError(f"event missing {key}")
    payload = dict(row)
    payload.setdefault("id", f"event-{fallback_seq}")
    payload.setdefault("generation", "default")
    payload.setdefault("timestamp", fallback_seq)
    payload.setdefault("seq", fallback_seq)
    payload.setdefault("mode", "exclusive")
    payload.setdefault("stack", [])
    payload.setdefault("broadcast", False)
    return payload


def insert_event(connection: sqlite3.Connection, branch_id: str, row: Dict[str, Any], arrival_seq: int) -> Tuple[str, float]:
    count = connection.execute("SELECT COUNT(*) AS count FROM events WHERE branch_id = ?", (branch_id,)).fetchone()["count"]
    payload = prepare_event(row, arrival_seq if arrival_seq >= 0 else count + 1)
    if arrival_seq < 0:
        max_seq = connection.execute(
            "SELECT COALESCE(MAX(arrival_seq), -1) AS seq FROM events WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()["seq"]
        payload["seq"] = max_seq + 1
    stack = payload.get("stack") or []
    params = (
        branch_id,
        str(payload["id"]),
        str(payload["generation"]),
        float(payload["timestamp"]),
        int(payload["seq"]),
        str(payload["kind"]),
        str(payload["thread"]),
        None if payload.get("lock") is None else str(payload["lock"]),
        str(payload.get("mode", "exclusive")),
        None if payload.get("request_id") is None else str(payload["request_id"]),
        None if payload.get("cond") is None else str(payload["cond"]),
        1 if payload.get("broadcast") else 0,
        json.dumps(stack),
        json.dumps(payload),
    )
    connection.execute(
        """
        INSERT INTO events(
            branch_id, event_id, generation, timestamp, arrival_seq, kind, thread,
            lock_address, mode, request_id, cond, broadcast, stack_json, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        params,
    )
    return str(payload["id"]), float(payload["timestamp"])


def append_events(connection: sqlite3.Connection, branch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    get_branch(connection, branch_id)
    ids = []
    min_time_by_gen: Dict[str, float] = {}
    for row in rows:
        event_id, timestamp = insert_event(connection, branch_id, row, -1)
        ids.append(event_id)
        generation = str(row.get("generation", "default"))
        if generation not in min_time_by_gen or timestamp < min_time_by_gen[generation]:
            min_time_by_gen[generation] = timestamp
    connection.commit()
    existing = load_events(connection, branch_id)
    reused = 0
    for generation, timestamp in min_time_by_gen.items():
        reused += sum(1 for event in existing if event["generation"] == generation and event["timestamp"] < timestamp and event["id"] not in ids)
    analysis = analyze(existing, load_lock_types(connection, branch_id), reused_events=reused)
    return {"branch_id": branch_id, "event_ids": ids, "analysis": analysis}


def set_lock_type(connection: sqlite3.Connection, branch_id: str, generation: str, lock: str, lock_type: str) -> None:
    if lock_type not in LOCK_TYPES:
        raise ValueError("unsupported lock type")
    get_branch(connection, branch_id)
    connection.execute(
        """
        INSERT INTO lock_types(branch_id, generation, lock_address, lock_type)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(branch_id, generation, lock_address) DO UPDATE SET lock_type=excluded.lock_type
        """,
        (branch_id, generation, lock, lock_type),
    )
    connection.commit()


def clone_branch(
    connection: sqlite3.Connection,
    source_id: str,
    new_id: str,
    name: str,
    note: str = "",
) -> Dict[str, Any]:
    get_branch(connection, source_id)
    try:
        connection.execute(
            "INSERT INTO branches(id, parent_id, name, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (new_id, source_id, name, note, time.time()),
        )
        connection.execute(
            "INSERT INTO events SELECT ?, event_id, generation, timestamp, arrival_seq, kind, thread, lock_address, mode, request_id, cond, broadcast, stack_json, payload_json FROM events WHERE branch_id = ?",
            (new_id, source_id),
        )
        connection.execute(
            "INSERT INTO lock_types SELECT ?, generation, lock_address, lock_type FROM lock_types WHERE branch_id = ?",
            (new_id, source_id),
        )
        connection.commit()
    except sqlite3.IntegrityError:
        connection.rollback()
        raise ValueError("branch id already exists")
    return get_branch(connection, new_id)


def correct_event(connection: sqlite3.Connection, branch_id: str, event_id: str, patch: Dict[str, Any], new_branch_id: str, name: str) -> Dict[str, Any]:
    get_branch(connection, branch_id)
    row = connection.execute("SELECT * FROM events WHERE branch_id = ? AND event_id = ?", (branch_id, event_id)).fetchone()
    if row is None:
        raise KeyError("unknown event")
    clone_branch(connection, branch_id, new_branch_id, name, f"correction:{event_id}")
    payload = event_payload(row)
    payload.update(patch)
    stack = payload.get("stack") or []
    connection.execute(
        """
        UPDATE events
        SET generation = ?, timestamp = ?, arrival_seq = ?, kind = ?, thread = ?, lock_address = ?,
            mode = ?, request_id = ?, cond = ?, broadcast = ?, stack_json = ?, payload_json = ?
        WHERE branch_id = ? AND event_id = ?
        """,
        (
            str(payload.get("generation", "default")),
            float(payload.get("timestamp", 0)),
            int(payload.get("seq", 0)),
            str(payload["kind"]),
            str(payload["thread"]),
            None if payload.get("lock") is None else str(payload["lock"]),
            str(payload.get("mode", "exclusive")),
            None if payload.get("request_id") is None else str(payload["request_id"]),
            None if payload.get("cond") is None else str(payload["cond"]),
            1 if payload.get("broadcast") else 0,
            json.dumps(stack),
            json.dumps(payload),
            new_branch_id,
            event_id,
        ),
    )
    connection.commit()
    return get_branch(connection, new_branch_id)
