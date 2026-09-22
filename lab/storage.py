"""SQLite 持久化：分支 / 事件 / 锁类型声明。"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from .model import Event, event_without_seq, normalize_event


SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES branches(id),
    note TEXT DEFAULT '',
    correction TEXT DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    thread TEXT,
    lock_addr TEXT,
    ts REAL,
    gen TEXT DEFAULT 'g0',
    cond TEXT,
    mode TEXT DEFAULT 'write',
    stack TEXT DEFAULT '[]',
    source_id TEXT,
    payload TEXT DEFAULT '{}',
    UNIQUE(branch_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_branch_seq ON events(branch_id, seq);
CREATE TABLE IF NOT EXISTS declarations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    lock_addr TEXT NOT NULL,
    kind TEXT NOT NULL,
    UNIQUE(branch_id, lock_addr)
);
"""


def connect(path: str = "lab.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
    row = conn.execute("SELECT id FROM branches WHERE name = 'main'").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO branches (name, parent_id, note) VALUES ('main', NULL, ?)",
            ("主干",),
        )
        conn.commit()


def list_branches(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, name, parent_id, note, correction FROM branches ORDER BY id"
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["correction"] = json.loads(item["correction"] or "{}")
        result.append(item)
    return result


def get_branch(conn: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM branches WHERE name = ?", (name,)
    ).fetchone()


def require_branch(conn: sqlite3.Connection, name: str) -> sqlite3.Row:
    row = get_branch(conn, name)
    if row is None:
        raise KeyError("branch not found: %s" % name)
    return row


def declarations(conn: sqlite3.Connection, branch_id: int) -> Dict[str, str]:
    rows = conn.execute(
        "SELECT lock_addr, kind FROM declarations WHERE branch_id = ?",
        (branch_id,),
    ).fetchall()
    return {row["lock_addr"]: row["kind"] for row in rows}


def next_seq(conn: sqlite3.Connection, branch_id: int) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE branch_id = ?",
        (branch_id,),
    ).fetchone()
    return int(row["m"]) + 1


def _insert_event(conn: sqlite3.Connection, branch_id: int, event: Event) -> None:
    conn.execute(
        """INSERT INTO events
           (branch_id, seq, type, thread, lock_addr, ts, gen, cond, mode,
            stack, source_id, payload)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            branch_id,
            event.seq,
            event.type,
            event.thread,
            event.lock,
            event.ts,
            event.gen,
            event.cond,
            event.mode,
            json.dumps(event.stack, ensure_ascii=False),
            event.source_id,
            json.dumps(event.raw, ensure_ascii=False),
        ),
    )


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        seq=row["seq"],
        type=row["type"],
        thread=row["thread"] or "",
        lock=row["lock_addr"],
        ts=row["ts"],
        gen=row["gen"] or "g0",
        cond=row["cond"],
        mode=row["mode"] or "write",
        stack=json.loads(row["stack"] or "[]"),
        source_id=row["source_id"],
        raw=json.loads(row["payload"] or "{}"),
    )


def load_events(conn: sqlite3.Connection, branch_id: int) -> List[Event]:
    rows = conn.execute(
        "SELECT * FROM events WHERE branch_id = ? ORDER BY seq", (branch_id,)
    ).fetchall()
    return [_row_to_event(row) for row in rows]


def append_events(
    conn: sqlite3.Connection,
    branch_id: int,
    items: List[Dict[str, Any]],
    *,
    base_seq: Optional[int] = None,
) -> List[Event]:
    """归一化并按到达顺序写入，返回落库事件。"""
    existing = load_events(conn, branch_id)
    prev_ts = existing[-1].ts if existing else None
    seq = (base_seq or 0) if base_seq is not None else next_seq(conn, branch_id) - 1
    normalized: List[Event] = []
    for item in items:
        seq += 1
        normalized.append(normalize_event(item, seq, prev_ts=prev_ts))
        if normalized[-1].ts is not None:
            prev_ts = normalized[-1].ts
    for event in normalized:
        _insert_event(conn, branch_id, event)
    conn.commit()
    return normalized


def replace_all_events(
    conn: sqlite3.Connection, branch_id: int, items: List[Dict[str, Any]]
) -> List[Event]:
    conn.execute("DELETE FROM events WHERE branch_id = ?", (branch_id,))
    conn.commit()
    return append_events(conn, branch_id, items, base_seq=0)


# ---- 分支（校正） ---------------------------------------------------------


def _resolve_target(
    target: Any, by_source: Dict[str, int], by_seq: Dict[int, int]
) -> Optional[int]:
    if target is None:
        return None
    if isinstance(target, int):
        return by_seq.get(target)
    text = str(target)
    if text in by_source:
        return by_source[text]
    try:
        return by_seq.get(int(text))
    except ValueError:
        return None


def materialize_branch(
    conn: sqlite3.Connection,
    parent_name: str,
    new_name: str,
    correction: Dict[str, Any],
    note: str = "",
) -> Dict[str, Any]:
    """基于父分支应用校正创建新分支；并列合法重建各自独立保留。"""
    parent = require_branch(conn, parent_name)
    if get_branch(conn, new_name) is not None:
        raise ValueError("branch already exists: %s" % new_name)

    cur = conn.execute(
        "INSERT INTO branches (name, parent_id, note, correction) VALUES (?,?,?,?)",
        (new_name, parent["id"], note, json.dumps(correction, ensure_ascii=False)),
    )
    new_id = int(cur.lastrowid)

    parent_events = load_events(conn, parent["id"])
    by_source = {e.source_id: e.seq for e in parent_events if e.source_id}
    by_seq = {e.seq: e.seq for e in parent_events}

    # 锁类型声明（继承父分支 + 校正覆盖）
    inherited = declarations(conn, parent["id"])
    for lock, kind in (correction.get("declarations") or {}).items():
        inherited[str(lock)] = str(kind)
    for lock, kind in inherited.items():
        conn.execute(
            """INSERT INTO declarations (branch_id, lock_addr, kind)
               VALUES (?,?,?)
               ON CONFLICT(branch_id, lock_addr) DO UPDATE SET kind=excluded.kind""",
            (new_id, lock, kind),
        )

    # 事件校正
    updates = {
        int(_resolve_target(k, by_source, by_seq) or -1): v
        for k, v in (correction.get("updates") or {}).items()
    }
    removes = {
        _resolve_target(k, by_source, by_seq)
        for k in (correction.get("remove") or [])
    }
    removes.discard(None)
    inserts = correction.get("insert") or []
    if isinstance(inserts, dict):
        inserts = [inserts]
    insert_after = correction.get("insert_after")

    new_payloads: Dict[int, Event] = {}
    parent_to_new: Dict[int, int] = {}
    seq = 0
    for event in parent_events:
        if event.seq in removes:
            continue
        seq += 1
        payload = event
        if event.seq in updates and updates[event.seq] is not None:
            changed = {
                k: v
                for k, v in updates[event.seq].items()
                if k in ("type", "thread", "lock", "ts", "gen", "cond", "mode", "stack")
            }
            payload = Event(**{**event.__dict__, **changed})
        payload.seq = seq
        new_payloads[seq] = payload
        parent_to_new[event.seq] = seq

    insert_after_parent = (
        _resolve_target(insert_after, by_source, by_seq)
        if insert_after is not None
        else None
    )
    if inserts and insert_after_parent is not None:
        position = parent_to_new.get(insert_after_parent)
        if position is not None:
            insert_items = [
                normalize_event(
                    event_without_seq(item),
                    0,
                    prev_ts=new_payloads[position].ts,
                )
                for item in inserts
            ]
            shifted = {
                old + len(insert_items): payload
                for old, payload in new_payloads.items()
                if old > position
            }
            for old in list(new_payloads):
                if old > position:
                    del new_payloads[old]
            for offset, item in enumerate(insert_items, start=1):
                item.seq = position + offset
                new_payloads[position + offset] = item
            for old, payload in shifted.items():
                payload.seq = old
                new_payloads[old] = payload

    if insert_after is None and inserts:
        for item in inserts:
            seq += 1
            new_payloads[seq] = normalize_event(
                event_without_seq(item), seq,
                prev_ts=parent_events[-1].ts if parent_events else None,
            )

    for event in new_payloads.values():
        _insert_event(conn, new_id, event)
    conn.commit()
    return {"id": new_id, "name": new_name, "events": len(new_payloads)}
