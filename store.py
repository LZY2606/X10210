"""SQLite 持久化：事件与分支（校正 / 锁类型声明）。"""
import json
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gen INTEGER NOT NULL,
    arrival INTEGER NOT NULL,
    kind TEXT NOT NULL,
    ts REAL NOT NULL,
    thread TEXT, lock TEXT, mode TEXT, cond TEXT, timeout REAL
);
CREATE TABLE IF NOT EXISTS branches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    corrections TEXT NOT NULL DEFAULT '[]',
    lock_types TEXT NOT NULL DEFAULT '{}'
);
"""


class Store:
    def __init__(self, path="locklab.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        if not self.conn.execute("SELECT 1 FROM branches WHERE id=0").fetchone():
            self.conn.execute(
                "INSERT INTO branches(id,name,corrections,lock_types) VALUES(0,'base','[]','{}')")
            self.conn.commit()

    def reset(self):
        self.conn.execute("DELETE FROM events")
        self.conn.execute("DELETE FROM branches WHERE id<>0")
        self.conn.commit()

    def add_events(self, events):
        cur = self.conn.cursor()
        ids = []
        for ev in events:
            cur.execute(
                "INSERT INTO events(gen,arrival,kind,ts,thread,lock,mode,cond,timeout)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (ev.get("gen", 0), ev.get("arrival", 0), ev["kind"], ev["ts"],
                 ev.get("thread"), ev.get("lock"), ev.get("mode"),
                 ev.get("cond"), ev.get("timeout")))
            ids.append(cur.lastrowid)
        self.conn.commit()
        return ids

    def all_events(self, gen=None):
        sql = "SELECT * FROM events"
        args = ()
        if gen is not None:
            sql += " WHERE gen=?"
            args = (gen,)
        rows = self.conn.execute(sql + " ORDER BY id", args).fetchall()
        return [dict(r) for r in rows]

    def create_branch(self, name, corrections, lock_types):
        cur = self.conn.execute(
            "INSERT INTO branches(name,corrections,lock_types) VALUES(?,?,?)",
            (name, json.dumps(corrections), json.dumps(lock_types)))
        self.conn.commit()
        return cur.lastrowid

    def branches(self):
        rows = self.conn.execute("SELECT * FROM branches ORDER BY id").fetchall()
        out = []
        for r in rows:
            out.append({"id": r["id"], "name": r["name"],
                        "corrections": json.loads(r["corrections"]),
                        "lock_types": json.loads(r["lock_types"])})
        return out

    def branch(self, bid):
        r = self.conn.execute("SELECT * FROM branches WHERE id=?", (bid,)).fetchone()
        if not r:
            return None
        return {"id": r["id"], "name": r["name"],
                "corrections": json.loads(r["corrections"]),
                "lock_types": json.loads(r["lock_types"])}
