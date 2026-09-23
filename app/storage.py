"""SQLite 存储层。

只持久化原始事件、分支与校正；派生的回放结果放在内存缓存中，事件变化即失效。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional

from .model import LOCK_TYPES, MUTEX, Event, normalize_event

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    seq         INTEGER NOT NULL,          -- 采集代次内的到达序号
    ts          REAL NOT NULL,
    generation  TEXT NOT NULL,
    type        TEXT NOT NULL,
    thread      TEXT NOT NULL,
    lock        TEXT,
    mode        TEXT,
    cv          TEXT,
    granted     INTEGER,
    stack       TEXT,
    wait_stack  TEXT,
    hold_stack  TEXT,
    note        TEXT,
    raw         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_order
    ON raw_events(generation, ts, seq);

CREATE TABLE IF NOT EXISTS lock_types (
    generation TEXT NOT NULL,
    lock_addr  TEXT NOT NULL,
    lock_type  TEXT NOT NULL,
    source     TEXT NOT NULL DEFAULT 'user',
    PRIMARY KEY (generation, lock_addr)
);

CREATE TABLE IF NOT EXISTS branches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    parent_id   INTEGER,                  -- NULL 表示主干
    note        TEXT,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS corrections (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id     INTEGER NOT NULL,
    action        TEXT NOT NULL,          -- update | add | delete | declare
    target_event  INTEGER,                -- update/delete 指向 raw_events.id
    payload       TEXT NOT NULL,          -- add/update 的事件 JSON / declare 的锁类型 JSON
    seq           INTEGER NOT NULL,
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corr_branch ON corrections(branch_id);
"""


def _event_from_row(row: sqlite3.Row) -> Event:
    def _loads(v):
        return json.loads(v) if v else None

    return Event(
        id=row["id"], seq=row["seq"], ts=row["ts"],
        generation=row["generation"], type=row["type"],
        thread=row["thread"], lock=row["lock"], mode=row["mode"],
        cv=row["cv"], granted=bool(row["granted"]),
        stack=_loads(row["stack"]), wait_stack=_loads(row["wait_stack"]),
        hold_stack=_loads(row["hold_stack"]), note=row["note"],
    )


class Store:
    def __init__(self, db_path: str | Path = "data.db"):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True) if (
            Path(self.db_path).parent.parts) else None
        with self._connect() as con:
            con.executescript(SCHEMA)
            cur = con.execute("SELECT COUNT(*) FROM branches")
            if cur.fetchone()[0] == 0:
                con.execute(
                    "INSERT INTO branches(id, name, parent_id, note, created_at) "
                    "VALUES (1, '主干', NULL, '原始采集事件流', 0)")
                con.execute(
                    "UPDATE sqlite_sequence SET seq = 1 WHERE name = 'branches'")

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        try:
            with self._lock:
                yield con
                con.commit()
        finally:
            con.close()

    # -- 事件 ---------------------------------------------------------------

    def insert_events(self, raws: Iterable[dict],
                      default_gen: str = "gen-0") -> list[Event]:
        """按代次保留原始到达顺序；返回规范化后的事件（含 id）。"""
        result: list[Event] = []
        with self._connect() as con:
            seq_by_gen: dict[str, int] = {}
            rows = con.execute(
                "SELECT generation, COALESCE(MAX(seq), 0) FROM raw_events "
                "GROUP BY generation").fetchall()
            seq_by_gen = {r[0]: r[1] for r in rows}
            for idx, raw in enumerate(raws):
                gen = str(raw.get("generation") or raw.get("gen")
                          or raw.get("generation_id") or default_gen)
                nxt = seq_by_gen.get(gen, 0) + 1
                seq_by_gen[gen] = nxt
                ev = normalize_event(raw, event_id=0, seq=nxt, default_gen=gen)
                con.execute(
                    "INSERT INTO raw_events(seq, ts, generation, type, thread, "
                    "lock, mode, cv, granted, stack, wait_stack, hold_stack, "
                    "note, raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ev.seq, ev.ts, ev.generation, ev.type, ev.thread, ev.lock,
                     ev.mode, ev.cv, int(ev.granted),
                     json.dumps(ev.stack, ensure_ascii=False),
                     json.dumps(ev.wait_stack, ensure_ascii=False),
                     json.dumps(ev.hold_stack, ensure_ascii=False),
                     ev.note, json.dumps(raw, ensure_ascii=False)))
                con.execute("SELECT last_insert_rowid()")
                ev.id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
                result.append(ev)
        return result

    def raw_events(self, generation: Optional[str] = None) -> list[Event]:
        sql = "SELECT * FROM raw_events"
        args: tuple = ()
        if generation is not None:
            sql += " WHERE generation = ?"
            args = (generation,)
        sql += " ORDER BY generation, seq"
        with self._connect() as con:
            return [_event_from_row(r) for r in con.execute(sql, args).fetchall()]

    def get_event(self, event_id: int) -> Optional[Event]:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM raw_events WHERE id = ?", (event_id,)).fetchone()
            return _event_from_row(row) if row else None

    def raw_payload(self, event_id: int) -> Optional[dict]:
        with self._connect() as con:
            row = con.execute(
                "SELECT raw FROM raw_events WHERE id = ?",
                (event_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def event_stats(self) -> dict:
        with self._connect() as con:
            gens = con.execute(
                "SELECT generation, COUNT(*), MIN(ts), MAX(ts) "
                "FROM raw_events GROUP BY generation ORDER BY generation"
            ).fetchall()
            return {
                "total": con.execute(
                    "SELECT COUNT(*) FROM raw_events").fetchone()[0],
                "generations": [
                    {"generation": g[0], "count": g[1],
                     "min_ts": g[2], "max_ts": g[3]} for g in gens],
            }

    def clear_all(self) -> None:
        with self._connect() as con:
            con.executescript(
                "DELETE FROM raw_events; DELETE FROM corrections; "
                "DELETE FROM lock_types; DELETE FROM branches;")
            con.execute(
                "INSERT INTO branches(id, name, parent_id, note, created_at) "
                "VALUES (1, '主干', NULL, '原始采集事件流', 0)")
            con.execute(
                "UPDATE sqlite_sequence SET seq = 1 WHERE name = 'branches'")

    # -- 锁类型声明 ----------------------------------------------------------

    def declare_lock_type(self, generation: str, lock: str, lock_type: str,
                          source: str = "user") -> None:
        if lock_type not in LOCK_TYPES:
            raise ValueError(f"未知锁类型: {lock_type}")
        with self._connect() as con:
            con.execute(
                "INSERT INTO lock_types(generation, lock_addr, lock_type, source) "
                "VALUES(?,?,?,?) ON CONFLICT(generation, lock_addr) DO UPDATE SET "
                "lock_type=excluded.lock_type, source=excluded.source",
                (generation, lock, lock_type, source))

    def declared_types(self) -> dict[tuple[str, str], str]:
        with self._connect() as con:
            return {(r["generation"], r["lock_addr"]): r["lock_type"]
                    for r in con.execute("SELECT * FROM lock_types")}

    # -- 分支与校正 ----------------------------------------------------------

    def branches(self) -> list[dict]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT b.*, (SELECT COUNT(*) FROM corrections c "
                "WHERE c.branch_id = b.id) AS n_corr FROM branches b "
                "ORDER BY b.id").fetchall()
            return [dict(r) for r in rows]

    def get_branch(self, branch_id: int) -> Optional[dict]:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM branches WHERE id = ?",
                (branch_id,)).fetchone()
            return dict(row) if row else None

    def create_branch(self, name: str, parent_id: int, note: str = "") -> int:
        with self._connect() as con:
            if con.execute("SELECT 1 FROM branches WHERE id=?",
                           (parent_id,)).fetchone() is None:
                raise ValueError(f"父分支不存在: {parent_id}")
            import time
            cur = con.execute(
                "INSERT INTO branches(name, parent_id, note, created_at) "
                "VALUES(?,?,?,?)", (name, parent_id, note, time.time()))
            return cur.lastrowid

    def add_correction(self, branch_id: int, action: str,
                       target_event: Optional[int], payload: dict,
                       seq: Optional[int] = None) -> int:
        if action not in {"update", "add", "delete", "declare"}:
            raise ValueError(f"未知校正动作: {action}")
        with self._connect() as con:
            if con.execute("SELECT 1 FROM branches WHERE id=?",
                           (branch_id,)).fetchone() is None:
                raise ValueError(f"分支不存在: {branch_id}")
            if branch_id == 1:
                raise ValueError("主干分支不可直接校正，请基于它创建新分支")
            import time
            if seq is None:
                row = con.execute(
                    "SELECT COALESCE(MAX(seq),0) FROM corrections "
                    "WHERE branch_id=?", (branch_id,)).fetchone()
                seq = row[0] + 1
            cur = con.execute(
                "INSERT INTO corrections(branch_id, action, target_event, "
                "payload, seq, created_at) VALUES(?,?,?,?,?,?)",
                (branch_id, action, target_event,
                 json.dumps(payload, ensure_ascii=False), seq, time.time()))
            return cur.lastrowid

    def corrections(self, branch_id: int) -> list[dict]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM corrections WHERE branch_id=? ORDER BY seq",
                (branch_id,)).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["payload"] = json.loads(d["payload"])
                out.append(d)
            return out

    def lineage(self, branch_id: int) -> list[int]:
        """从主干到该分支的分支 id 链。"""
        chain: list[int] = []
        cur = branch_id
        with self._connect() as con:
            seen = set()
            while cur is not None and cur not in seen:
                seen.add(cur)
                chain.append(cur)
                row = con.execute(
                    "SELECT parent_id FROM branches WHERE id=?",
                    (cur,)).fetchone()
                if row is None:
                    break
                cur = row[0]
        return list(reversed(chain))
