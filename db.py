"""SQLite 存储层：代次、事件、分支与校正。"""
import os
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS generations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    ts REAL,
    kind TEXT NOT NULL,
    thread TEXT,
    lock TEXT,
    extra TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_gen ON events(generation_id, seq);
CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    generation_id INTEGER NOT NULL,
    ctype TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""


def db_path():
    return os.environ.get("LOCKLAB_DB", os.path.join(os.getcwd(), "locklab.db"))


def get_db():
    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT id FROM branches WHERE name = 'main'").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO branches(name, parent_id, created_at) VALUES('main', NULL, ?)",
            (time.time(),),
        )
        conn.commit()
    return conn
