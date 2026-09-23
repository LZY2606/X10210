"""SQLite 持久化：runs / events / branches / 锁类型声明。"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Optional

from . import engine

DB_PATH = "locklab.db"


def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at REAL NOT NULL,
            last_seq INTEGER NOT NULL DEFAULT 0,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            branch_id TEXT NOT NULL DEFAULT 'main',
            gen TEXT NOT NULL,
            ts REAL NOT NULL,
            seq INTEGER NOT NULL,
            type TEXT NOT NULL,
            thread TEXT NOT NULL,
            lock TEXT,
            mode TEXT,
            cond TEXT,
            notify_all INTEGER,
            stack TEXT,
            payload TEXT NOT NULL,
            arrival_ts REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, branch_id, gen, ts, seq);
        CREATE TABLE IF NOT EXISTS branches (
            id TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            parent_id TEXT,
            label TEXT NOT NULL,
            correction TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (run_id, id)
        );
        CREATE TABLE IF NOT EXISTS lock_types (
            run_id TEXT NOT NULL,
            branch_id TEXT NOT NULL DEFAULT 'main',
            lock_key TEXT NOT NULL,
            type TEXT NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (run_id, branch_id, lock_key)
        );
        """
    )
    conn.commit()


def create_run(conn: sqlite3.Connection, name: str, note: str = "") -> dict:
    run_id = uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO runs(id, name, created_at, note) VALUES (?,?,?,?)",
        (run_id, name, time.time(), note),
    )
    conn.execute(
        "INSERT INTO branches(run_id, id, parent_id, label, correction, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (run_id, "main", None, "主干重建", "{}", time.time()),
    )
    conn.commit()
    return get_run(conn, run_id)


def get_run(conn: sqlite3.Connection, run_id: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def list_runs(conn: sqlite3.Connection) -> list:
    rows = conn.execute("SELECT * FROM runs ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def next_seq(conn: sqlite3.Connection, run_id: str, branch_id: str = "main") -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE run_id=? AND branch_id=?",
        (run_id, branch_id),
    ).fetchone()
    return int(row["m"]) + 1


def insert_event(conn, run_id, raw, branch_id="main", seq=None) -> dict:
    ev = engine.make_event(raw, seq if seq is not None else 0, None)
    use_seq = seq if seq is not None else next_seq(conn, run_id, branch_id)
    if seq is None and ev.seq:
        use_seq = int(ev.seq)
    cur = conn.execute(
        """INSERT INTO events
           (run_id, branch_id, gen, ts, seq, type, thread, lock, mode, cond,
            notify_all, stack, payload, arrival_ts)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id, branch_id, ev.gen, ev.ts, use_seq, ev.type, ev.thread,
            ev.lock, ev.mode, ev.cond, 1 if ev.notify_all else 0,
            json.dumps(list(ev.stack), ensure_ascii=False),
            json.dumps(raw, ensure_ascii=False),
            time.time(),
        ),
    )
    conn.execute("UPDATE runs SET last_seq=? WHERE id=?", (use_seq, run_id))
    conn.commit()
    row = conn.execute("SELECT * FROM events WHERE id=?", (cur.lastrowid,)).fetchone()
    return _event_row_to_dict(row)


def insert_events_bulk(conn, run_id, raws, branch_id="main") -> list:
    out = []
    base = next_seq(conn, run_id, branch_id) - 1
    for i, raw in enumerate(raws):
        ev = engine.make_event(raw, base + i + 1, None)
        seq = int(raw.get("seq") if raw.get("seq") is not None else base + i + 1)
        cur = conn.execute(
            """INSERT INTO events
               (run_id, branch_id, gen, ts, seq, type, thread, lock, mode, cond,
                notify_all, stack, payload, arrival_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id, branch_id, ev.gen, ev.ts, seq, ev.type, ev.thread,
                ev.lock, ev.mode, ev.cond, 1 if ev.notify_all else 0,
                json.dumps(list(ev.stack), ensure_ascii=False),
                json.dumps(raw, ensure_ascii=False),
                time.time(),
            ),
        )
        out.append(cur.lastrowid)
    conn.execute(
        "UPDATE runs SET last_seq=(SELECT COALESCE(MAX(seq),0) FROM events WHERE run_id=?) WHERE id=?",
        (run_id, run_id),
    )
    conn.commit()
    return out


def _event_row_to_dict(row) -> dict:
    d = dict(row)
    d["notify_all"] = bool(d.get("notify_all"))
    try:
        d["stack"] = json.loads(d.get("stack") or "[]")
    except (TypeError, ValueError):
        d["stack"] = []
    return d


def load_events(conn, run_id, branch_id="main") -> list:
    rows = conn.execute(
        "SELECT * FROM events WHERE run_id=? AND branch_id=? ORDER BY gen, ts, seq, id",
        (run_id, branch_id),
    ).fetchall()
    events = []
    for row in rows:
        d = _event_row_to_dict(row)
        raw = json.loads(d["payload"])
        raw.setdefault("id", d["id"])
        ev = engine.make_event(raw, d["seq"], event_id=d["id"])
        ev.gen = d["gen"]
        ev.ts = d["ts"]
        ev.seq = d["seq"]
        events.append(ev)
    return events


def list_branches(conn, run_id) -> list:
    rows = conn.execute(
        "SELECT * FROM branches WHERE run_id=? ORDER BY created_at", (run_id,)
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["correction"] = json.loads(d["correction"])
        out.append(d)
    return out


def create_branch(conn, run_id, parent_id, label, correction) -> dict:
    branch_id = uuid.uuid4().hex[:10]
    conn.execute(
        "INSERT INTO branches(run_id, id, parent_id, label, correction, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (run_id, branch_id, parent_id, label, json.dumps(correction, ensure_ascii=False), time.time()),
    )
    # 复制父分支事件到新分支
    conn.execute(
        "INSERT INTO events(run_id, branch_id, gen, ts, seq, type, thread, lock, mode, "
        "cond, notify_all, stack, payload, arrival_ts) "
        "SELECT ?, ?, gen, ts, seq, type, thread, lock, mode, cond, notify_all, stack, "
        "payload, arrival_ts FROM events WHERE run_id=? AND branch_id=?",
        (run_id, branch_id, run_id, parent_id),
    )
    conn.execute(
        "INSERT OR IGNORE INTO lock_types(run_id, branch_id, lock_key, type, updated_at) "
        "SELECT ?, ?, lock_key, type, updated_at FROM lock_types WHERE run_id=? AND branch_id=?",
        (run_id, branch_id, run_id, parent_id),
    )
    conn.commit()
    return {"id": branch_id, "run_id": run_id, "parent_id": parent_id,
            "label": label, "correction": correction}


def set_lock_type(conn, run_id, branch_id, gen, lock, lock_type):
    key = f"{gen}::{lock}"
    conn.execute(
        """INSERT INTO lock_types(run_id, branch_id, lock_key, type, updated_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(run_id, branch_id, lock_key)
           DO UPDATE SET type=excluded.type, updated_at=excluded.updated_at""",
        (run_id, branch_id, key, lock_type, time.time()),
    )
    conn.commit()


def declared_types(conn, run_id, branch_id="main") -> dict:
    rows = conn.execute(
        "SELECT lock_key, type FROM lock_types WHERE run_id=? AND branch_id=?",
        (run_id, branch_id),
    ).fetchall()
    return {r["lock_key"]: r["type"] for r in rows}
