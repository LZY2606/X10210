"""SQLite 存储层：事件按 (branch, epoch, seq) 保留原始顺序；分支保存校正。"""
from __future__ import annotations

import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS branches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent INTEGER,
    note TEXT DEFAULT '',
    created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    ts REAL NOT NULL DEFAULT 0,
    thread TEXT,
    lock TEXT,
    kind TEXT,
    wait_id TEXT,
    timeout_ms REAL,
    cond TEXT,
    broadcast INTEGER DEFAULT 0,
    corrected_from INTEGER
);
CREATE TABLE IF NOT EXISTS declarations(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    lock TEXT NOT NULL,
    ltype TEXT NOT NULL
);
"""

EVENT_COLS = ("id", "branch", "epoch", "seq", "type", "ts", "thread", "lock",
              "kind", "wait_id", "timeout_ms", "cond", "broadcast", "corrected_from")


def connect(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def ensure_main(conn):
    row = conn.execute("SELECT id FROM branches WHERE name='main'").fetchone()
    if row:
        return row["id"]
    cur = conn.execute("INSERT INTO branches(name, parent, note, created) VALUES('main', NULL, '', ?)",
                       (time.time(),))
    conn.commit()
    return cur.lastrowid


def _row_to_event(row):
    e = {c: row[c] for c in EVENT_COLS}
    e["all"] = bool(e.pop("broadcast"))
    return e


def list_events(conn, branch):
    rows = conn.execute(
        "SELECT * FROM events WHERE branch=? ORDER BY epoch, seq, id", (branch,)).fetchall()
    return [_row_to_event(r) for r in rows]


def insert_events(conn, branch, epoch, events):
    """按批内顺序赋 seq，保留采集原始顺序。"""
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), -1) AS m FROM events WHERE branch=? AND epoch=?",
        (branch, epoch)).fetchone()
    seq = row["m"] + 1
    ids = []
    for e in events:
        cur = conn.execute(
            "INSERT INTO events(branch, epoch, seq, type, ts, thread, lock, kind, wait_id,"
            " timeout_ms, cond, broadcast, corrected_from) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (branch, epoch, seq, e["type"], e.get("ts", 0), e.get("thread"), e.get("lock"),
             e.get("kind"), e.get("wait_id"), e.get("timeout_ms"), e.get("cond"),
             1 if e.get("all") else 0, e.get("corrected_from")))
        ids.append(cur.lastrowid)
        seq += 1
    conn.commit()
    return ids


def list_branches(conn):
    rows = conn.execute("SELECT * FROM branches ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def create_branch(conn, name, parent, corrections=None, declarations=None, note=""):
    """从父分支复制事件与声明，再应用校正（事件修补 / 锁类型声明），形成新分支。"""
    cur = conn.execute(
        "INSERT INTO branches(name, parent, note, created) VALUES(?,?,?,?)",
        (name, parent, note, time.time()))
    branch = cur.lastrowid
    parent_events = list_events(conn, parent)
    corrections = corrections or []
    patches = {}    # event_id -> {field: value}
    additions = []  # 新事件
    for c in corrections:
        if "event_id" in c:
            patches[c["event_id"]] = c.get("patch", {})
        elif "new_event" in c:
            additions.append(c["new_event"])
    for e in parent_events:
        patch = patches.get(e["id"], {})
        merged = dict(e)
        merged.update(patch)
        conn.execute(
            "INSERT INTO events(branch, epoch, seq, type, ts, thread, lock, kind, wait_id,"
            " timeout_ms, cond, broadcast, corrected_from) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (branch, merged["epoch"], merged["seq"], merged["type"], merged["ts"],
             merged.get("thread"), merged.get("lock"), merged.get("kind"),
             merged.get("wait_id"), merged.get("timeout_ms"), merged.get("cond"),
             1 if merged.get("all") else 0, e["id"] if patch else None))
    if additions:
        by_epoch = {}
        for ne in additions:
            by_epoch.setdefault(ne.get("epoch", 0), []).append(ne)
        for epoch, evs in by_epoch.items():
            insert_events(conn, branch, epoch, evs)
    # 声明：继承父分支 + 应用新声明
    rows = conn.execute("SELECT epoch, lock, ltype FROM declarations WHERE branch=?",
                        (parent,)).fetchall()
    inherited = {(r["epoch"], r["lock"]): r["ltype"] for r in rows}
    for d in declarations or []:
        inherited[(d["epoch"], d["lock"])] = d["ltype"]
    for (epoch, lock), ltype in inherited.items():
        conn.execute(
            "INSERT INTO declarations(branch, epoch, lock, ltype) VALUES(?,?,?,?)",
            (branch, epoch, lock, ltype))
    conn.commit()
    return branch


def get_declarations(conn, branch):
    rows = conn.execute("SELECT epoch, lock, ltype FROM declarations WHERE branch=?",
                        (branch,)).fetchall()
    return {(r["epoch"], r["lock"]): r["ltype"] for r in rows}


def add_declaration(conn, branch, epoch, lock, ltype):
    conn.execute("DELETE FROM declarations WHERE branch=? AND epoch=? AND lock=?",
                 (branch, epoch, lock))
    conn.execute("INSERT INTO declarations(branch, epoch, lock, ltype) VALUES(?,?,?,?)",
                 (branch, epoch, lock, ltype))
    conn.commit()
