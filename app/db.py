"""SQLite 存储：原始事件按采集代次与到达序保留，分支为校正后的事件视图。"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import Event, LOCK_KINDS

_LOCK = threading.RLock()


def connect(db_path: str = "lab.db") -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True) if Path(db_path).parent.parts else None
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    with _LOCK:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gen TEXT NOT NULL,
                seq INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                kind TEXT NOT NULL,
                thread TEXT NOT NULL,
                lock_addr TEXT,
                mode TEXT,
                gen_from TEXT,
                timeout INTEGER,
                stack TEXT NOT NULL DEFAULT '[]',
                created_order INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lock_types (
                lock_uid TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                declared_by TEXT NOT NULL DEFAULT 'system'
            );
            CREATE TABLE IF NOT EXISTS branches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                parent_id INTEGER,
                note TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS branch_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                branch_id INTEGER NOT NULL,
                event_id INTEGER,
                action TEXT NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(branch_id) REFERENCES branches(id)
            );
            """
        )
        conn.commit()


def row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        raw_id=row["id"],
        gen=row["gen"],
        seq=row["seq"],
        ts=row["ts"],
        kind=row["kind"],
        thread=row["thread"],
        lock=row["lock_addr"],
        mode=row["mode"],
        gen_from=row["gen_from"],
        timeout=row["timeout"],
        stack=json.loads(row["stack"] or "[]"),
    )


def insert_raw_events(
    conn: sqlite3.Connection, events: List[Dict[str, Any]]
) -> List[int]:
    ids: List[int] = []
    max_order = conn.execute(
        "SELECT COALESCE(MAX(created_order), 0) FROM raw_events"
    ).fetchone()[0]
    existing_seq = {
        (row["gen"], row["seq"])
        for row in conn.execute("SELECT gen, seq FROM raw_events")
    }
    with _LOCK:
        for offset, item in enumerate(events):
            gen = str(item.get("gen", "g1"))
            seq = item.get("seq")
            if seq is None:
                seq = (
                    max(s for g, s in existing_seq if g == gen) + 1
                    if any(g == gen for g, _ in existing_seq)
                    else 1
                )
            key = (gen, seq)
            if key in existing_seq:
                seq = (
                    max(s for g, s in existing_seq if g == gen) + 1
                    if any(g == gen for g, _ in existing_seq)
                    else 1
                )
                key = (gen, seq)
            existing_seq.add(key)
            max_order += 1
            cur = conn.execute(
                """INSERT INTO raw_events
                   (gen, seq, ts, kind, thread, lock_addr, mode, gen_from,
                    timeout, stack, created_order)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    gen,
                    int(seq),
                    int(item["ts"]),
                    item["kind"],
                    item["thread"],
                    item.get("lock"),
                    item.get("mode"),
                    item.get("gen_from"),
                    item.get("timeout"),
                    json.dumps(item.get("stack", [])),
                    max_order,
                ),
            )
            ids.append(cur.lastrowid)
        conn.commit()
    return ids


def load_raw_events(conn: sqlite3.Connection) -> List[Event]:
    rows = conn.execute(
        "SELECT * FROM raw_events ORDER BY created_order"
    ).fetchall()
    return [row_to_event(row) for row in rows]


def upsert_lock_type(
    conn: sqlite3.Connection, lock_uid: str, kind: str
) -> None:
    if kind not in LOCK_KINDS:
        raise ValueError(f"unsupported lock kind: {kind}")
    with _LOCK:
        conn.execute(
            """INSERT INTO lock_types(lock_uid, kind, declared_by)
               VALUES (?,?, 'user')
               ON CONFLICT(lock_uid) DO UPDATE SET kind=excluded.kind,
                                                   declared_by='user'""",
            (lock_uid, kind),
        )
        conn.commit()


def load_lock_types(conn: sqlite3.Connection) -> Dict[str, str]:
    return {
        row["lock_uid"]: row["kind"]
        for row in conn.execute("SELECT lock_uid, kind FROM lock_types")
    }


def reset_all(conn: sqlite3.Connection) -> None:
    with _LOCK:
        conn.executescript(
            "DELETE FROM branch_overrides; DELETE FROM branches; "
            "DELETE FROM lock_types; DELETE FROM raw_events;"
        )
        conn.commit()


# ---- 分支（事件校正） -----------------------------------------------------
def create_branch(
    conn: sqlite3.Connection,
    name: str,
    parent_id: Optional[int] = None,
    note: str = "",
) -> int:
    with _LOCK:
        cur = conn.execute(
            "INSERT INTO branches(name, parent_id, note) VALUES (?,?,?)",
            (name, parent_id, note),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_branches(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, name, parent_id, note, created_at FROM branches ORDER BY id"
    ).fetchall()
    return [dict(row) for row in rows]


def add_override(
    conn: sqlite3.Connection,
    branch_id: int,
    action: str,
    payload: Dict[str, Any],
    event_id: Optional[int] = None,
) -> int:
    with _LOCK:
        cur = conn.execute(
            """INSERT INTO branch_overrides(branch_id, event_id, action, payload)
               VALUES (?,?,?,?)""",
            (branch_id, event_id, action, json.dumps(payload)),
        )
        conn.commit()
        return int(cur.lastrowid)


def overrides_for(
    conn: sqlite3.Connection, branch_id: Optional[int]
) -> List[Dict[str, Any]]:
    if branch_id is None:
        return []
    rows = conn.execute(
        "SELECT * FROM branch_overrides WHERE branch_id=? ORDER BY id",
        (branch_id,),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["payload"] = json.loads(item["payload"] or "{}")
        result.append(item)
    return result


def apply_overrides(
    events: List[Event], overrides: List[Dict[str, Any]]
) -> List[Event]:
    """分支视图：按覆盖动作产出校正后的事件副本（原始表不动）。"""
    events = [
        Event(
            raw_id=event.raw_id,
            gen=event.gen,
            seq=event.seq,
            ts=event.ts,
            kind=event.kind,
            thread=event.thread,
            lock=event.lock,
            mode=event.mode,
            gen_from=event.gen_from,
            timeout=event.timeout,
            stack=list(event.stack),
        )
        for event in events
    ]
    by_id = {event.raw_id: event for event in events}
    appended: List[Event] = []
    for override in overrides:
        payload = override["payload"]
        if override["action"] == "delete_event":
            event = by_id.get(override["event_id"])
            if event is not None:
                event.deleted = True
        elif override["action"] == "mutate_event":
            event = by_id.get(override["event_id"])
            if event is None:
                continue
            for field_name in (
                "ts",
                "kind",
                "thread",
                "lock",
                "mode",
                "gen_from",
                "timeout",
            ):
                if field_name in payload:
                    setattr(event, field_name, payload[field_name])
            if "stack" in payload:
                event.stack = list(payload["stack"])
        elif override["action"] == "insert_event":
            new_event = Event(
                raw_id=-(len(appended) + 1),
                gen=str(payload.get("gen", "g1")),
                seq=int(payload.get("seq", 10_000 + len(appended))),
                ts=int(payload["ts"]),
                kind=payload["kind"],
                thread=payload["thread"],
                lock=payload.get("lock"),
                mode=payload.get("mode"),
                gen_from=payload.get("gen_from"),
                timeout=payload.get("timeout"),
                stack=list(payload.get("stack", [])),
            )
            appended.append(new_event)
    return events + appended
