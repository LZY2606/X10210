"""SQLite 存储：事件按采集代次保留原始顺序，校正形成新分支。"""
from __future__ import annotations

import json
import sqlite3
from typing import List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS branches(
    id INTEGER PRIMARY KEY,
    parent INTEGER,
    note TEXT
);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER NOT NULL,
    branch INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    ts REAL NOT NULL,
    thread TEXT,
    lock TEXT,
    type TEXT NOT NULL,
    extra TEXT,
    PRIMARY KEY(branch, id)
);
CREATE TABLE IF NOT EXISTS lock_types(
    branch INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    address TEXT NOT NULL,
    ltype TEXT NOT NULL,
    PRIMARY KEY(branch, generation, address)
);
"""
SCHEMA_META = """
CREATE TABLE IF NOT EXISTS meta(
    k TEXT PRIMARY KEY,
    v INTEGER
);
"""


class Store:
    def __init__(self, path: str = "locklab.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA_META + SCHEMA)
        if self.conn.execute("SELECT COUNT(*) c FROM branches").fetchone()["c"] == 0:
            self.conn.execute("INSERT INTO branches(id, parent, note) VALUES(0, NULL, 'base')")
            self.conn.execute("INSERT INTO meta(k, v) VALUES('next_event_id', 1)")
            self.conn.commit()

    def create_branch(self, parent: int, note: str) -> int:
        cur = self.conn.execute("INSERT INTO branches(parent, note) VALUES(?, ?)", (parent, note))
        bid = cur.lastrowid
        self.conn.execute(
            "INSERT INTO events(id, branch, generation, seq, ts, thread, lock, type, extra) "
            "SELECT id, ?, generation, seq, ts, thread, lock, type, extra FROM events WHERE branch=?",
            (bid, parent))
        self.conn.execute(
            "INSERT INTO lock_types(branch, generation, address, ltype) "
            "SELECT ?, generation, address, ltype FROM lock_types WHERE branch=?",
            (bid, parent))
        self.conn.commit()
        return bid

    def branches(self) -> List[dict]:
        rows = self.conn.execute("SELECT * FROM branches ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def import_events(self, branch: int, generation: int, events: List[dict]) -> List[int]:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq), -1) m FROM events WHERE branch=? AND generation=?",
            (branch, generation)).fetchone()
        seq = row["m"] + 1
        nid = self.conn.execute("SELECT v FROM meta WHERE k='next_event_id'").fetchone()["v"]
        ids = []
        for e in events:
            self.conn.execute(
                "INSERT INTO events(id, branch, generation, seq, ts, thread, lock, type, extra) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (nid, branch, generation, seq, e.get("ts", 0.0), e.get("thread"),
                 e.get("lock"), e["type"], json.dumps(e.get("extra") or {})))
            ids.append(nid)
            nid += 1
            seq += 1
        self.conn.execute("UPDATE meta SET v=? WHERE k='next_event_id'", (nid,))
        self.conn.commit()
        return ids

    def list_events(self, branch: int) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM events WHERE branch=? ORDER BY generation, seq", (branch,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["extra"] = json.loads(d["extra"] or "{}")
            out.append(d)
        return out

    def edit_event(self, branch: int, event_id: int, fields: dict) -> None:
        allowed = {"ts", "thread", "lock", "type", "generation"}
        sets, vals = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append("%s=?" % k)
                vals.append(v)
        if "extra" in fields:
            sets.append("extra=?")
            vals.append(json.dumps(fields["extra"]))
        if not sets:
            return
        vals.extend([event_id, branch])
        self.conn.execute(
            "UPDATE events SET %s WHERE id=? AND branch=?" % ", ".join(sets), vals)
        self.conn.commit()

    def declare_lock(self, branch: int, generation: int, address: str, ltype: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO lock_types(branch, generation, address, ltype) VALUES(?,?,?,?)",
            (branch, generation, address, ltype))
        self.conn.commit()

    def lock_types(self, branch: int) -> dict:
        rows = self.conn.execute(
            "SELECT generation, address, ltype FROM lock_types WHERE branch=?", (branch,)).fetchall()
        return {"%s:%s" % (r["generation"], r["address"]): r["ltype"] for r in rows}
