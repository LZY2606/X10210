"""SQLite 存储：原始事件只追加不可变；分支通过校正覆盖原事件视图。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Optional

from .engine import normalize_event

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT,
    arrival INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL,
    generation TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS branches (
    name TEXT PRIMARY KEY,
    parent TEXT,
    note TEXT DEFAULT '',
    created_at REAL DEFAULT (julianday('now'))
);
CREATE TABLE IF NOT EXISTS corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch TEXT NOT NULL,
    kind TEXT NOT NULL,           -- patch | ignore | insert | declare
    event_id TEXT,
    payload TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        self._tls = threading.local()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO branches(name, parent, note) "
                "VALUES ('main', NULL, '原始采集轨迹')")

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._tls.conn = conn
        return conn

    # ---- 事件 -----------------------------------------------------------
    def add_events(self, raw_events: list[dict]) -> int:
        conn = self._conn()
        before = conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]
        for raw in raw_events:
            arrival = self.next_arrival()
            ev = normalize_event(raw, arrival)
            conn.execute(
                "INSERT INTO events(id, payload, generation, timestamp) "
                "VALUES (?,?,?,?)",
                (ev["id"], json.dumps(raw, ensure_ascii=False),
                 ev["generation"], ev["timestamp"]))
        conn.commit()
        after = conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]
        return after - before

    def next_arrival(self) -> int:
        row = self._conn().execute(
            "SELECT COALESCE(MAX(arrival),0)+1 n FROM events").fetchone()
        return row["n"]

    def raw_events(self, branch: str = "main") -> list[dict]:
        """返回该分支可见的事件视图（原始顺序 = arrival）。"""
        conn = self._conn()
        rows = conn.execute(
            "SELECT id, arrival, payload FROM events ORDER BY arrival").fetchall()
        corrections = self.corrections_for_branch(branch)
        ignores = {c["event_id"] for c in corrections if c["kind"] == "ignore"}
        patches = {c["event_id"]: c["payload"]
                   for c in corrections if c["kind"] == "patch"}
        declares = [c["payload"] for c in corrections
                    if c["kind"] == "declare"]
        inserts = [c["payload"] for c in corrections
                   if c["kind"] == "insert"]
        out: list[dict] = []
        arrival = 0
        for row in rows:
            if row["id"] in ignores:
                continue
            payload = dict(json.loads(row["payload"]))
            if row["id"] in patches:
                payload = {**payload, **patches[row["id"]]}
            arrival += 1
            ev = normalize_event(payload, arrival)
            out.append(ev)
        for ins in inserts:
            arrival += 1
            out.append(normalize_event(ins, arrival))
        if declares:
            self._apply_declares(out, declares)
        return out

    @staticmethod
    def _apply_declares(events: list[dict], declares: list[dict]) -> None:
        # declare 不产生事件；锁类型通过事件上的 _declared_types 传递
        merged: dict[str, str] = {}
        for d in declares:
            merged[d["lock"]] = d["lock_type"]
        for ev in events:
            ev.setdefault("_declared_types", {}).update(merged)

    # ---- 分支 -----------------------------------------------------------
    def create_branch(self, name: str, parent: str, note: str = "") -> None:
        conn = self._conn()
        exists = conn.execute(
            "SELECT 1 FROM branches WHERE name=?", (name,)).fetchone()
        if exists:
            raise ValueError(f"branch already exists: {name}")
        if not conn.execute("SELECT 1 FROM branches WHERE name=?",
                            (parent,)).fetchone():
            raise ValueError(f"unknown parent branch: {parent}")
        conn.execute(
            "INSERT INTO branches(name, parent, note) VALUES (?,?,?)",
            (name, parent, note))
        conn.commit()

    def branches(self) -> list[dict]:
        rows = self._conn().execute(
            "SELECT name, parent, note FROM branches ORDER BY rowid").fetchall()
        return [dict(r) for r in rows]

    def add_correction(self, branch: str, kind: str,
                       payload: dict, event_id: Optional[str] = None) -> int:
        if kind not in {"patch", "ignore", "insert", "declare"}:
            raise ValueError(f"bad correction kind: {kind}")
        conn = self._conn()
        if not conn.execute("SELECT 1 FROM branches WHERE name=?",
                            (branch,)).fetchone():
            raise ValueError(f"unknown branch: {branch}")
        if branch == "main":
            raise ValueError("main 分支为原始轨迹，不能添加校正")
        cur = conn.execute(
            "INSERT INTO corrections(branch, kind, event_id, payload) "
            "VALUES (?,?,?,?)",
            (branch, kind, event_id, json.dumps(payload, ensure_ascii=False)))
        conn.commit()
        return cur.lastrowid

    def corrections_for_branch(self, branch: str) -> list[dict]:
        conn = self._conn()
        chain: list[str] = []
        cur = branch
        seen = set()
        while cur and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            row = conn.execute(
                "SELECT parent FROM branches WHERE name=?", (cur,)).fetchone()
            cur = row["parent"] if row else None
        placeholders = ",".join("?" for _ in chain)
        rows = conn.execute(
            f"SELECT branch, kind, event_id, payload FROM corrections "
            f"WHERE branch IN ({placeholders})", chain).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["payload"] = json.loads(r["payload"])
            out.append(item)
        return out
