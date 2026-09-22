"""SQLite 持久化：事件（按代次保留原始顺序）、分支与校正。"""
import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  ts REAL,
  type TEXT NOT NULL,
  thread TEXT,
  lock TEXT,
  cond TEXT,
  mode TEXT,
  timeout_ms REAL,
  extra TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_gen ON events(generation, seq);
CREATE TABLE IF NOT EXISTS branches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  parent INTEGER NOT NULL DEFAULT 0,
  created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS corrections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  op TEXT NOT NULL,
  payload TEXT NOT NULL
);
"""

KNOWN = {"id", "generation", "seq", "ts", "type", "thread", "lock", "cond",
         "mode", "timeout_ms", "extra"}


class Store:
    def __init__(self, path="locklab.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def add_events(self, events):
        added = []
        cur = self.conn.cursor()
        for ev in events:
            gen = int(ev.get("generation", 0))
            seq = ev.get("seq")
            if seq is None:
                row = cur.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE generation=?",
                    (gen,)).fetchone()
                seq = row["m"] + 1
            extra = {k: v for k, v in ev.items() if k not in KNOWN}
            cur.execute(
                "INSERT INTO events(generation, seq, ts, type, thread, lock, cond,"
                " mode, timeout_ms, extra) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (gen, seq, ev.get("ts"), ev.get("type"), ev.get("thread"),
                 ev.get("lock"), ev.get("cond"), ev.get("mode"),
                 ev.get("timeout_ms"),
                 json.dumps(extra, ensure_ascii=False) if extra else None))
            added.append(dict({k: v for k, v in ev.items() if k not in ("id", "seq")},
                              id=cur.lastrowid, generation=gen, seq=seq))
        self.conn.commit()
        return added

    def events(self, generation):
        rows = self.conn.execute(
            "SELECT * FROM events WHERE generation=? ORDER BY seq, id",
            (generation,)).fetchall()
        out = []
        for r in rows:
            ev = {"id": r["id"], "generation": r["generation"], "seq": r["seq"],
                  "ts": r["ts"], "type": r["type"], "thread": r["thread"],
                  "lock": r["lock"], "cond": r["cond"], "mode": r["mode"],
                  "timeout_ms": r["timeout_ms"]}
            if r["extra"]:
                ev.update(json.loads(r["extra"]))
            out.append({k: v for k, v in ev.items() if v is not None})
        return out

    def generations(self):
        rows = self.conn.execute(
            "SELECT DISTINCT generation FROM events ORDER BY generation").fetchall()
        return [r["generation"] for r in rows]

    def create_branch(self, name, parent, corrections):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO branches(name, parent, created) VALUES (?,?,?)",
                    (name, parent or 0, time.time()))
        bid = cur.lastrowid
        for c in corrections:
            cur.execute(
                "INSERT INTO corrections(branch_id, op, payload) VALUES (?,?,?)",
                (bid, c["op"], json.dumps(c.get("payload", {}), ensure_ascii=False)))
        self.conn.commit()
        return bid

    def branches(self):
        rows = self.conn.execute("SELECT * FROM branches ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def corrections_for(self, branch_id):
        chain = []
        bid = branch_id
        seen = set()
        while bid and bid not in seen:
            seen.add(bid)
            row = self.conn.execute(
                "SELECT * FROM branches WHERE id=?", (bid,)).fetchone()
            if not row:
                break
            chain.append(bid)
            bid = row["parent"]
        corrections = []
        for b in reversed(chain):
            rows = self.conn.execute(
                "SELECT * FROM corrections WHERE branch_id=? ORDER BY id",
                (b,)).fetchall()
            for r in rows:
                corrections.append(dict({"op": r["op"]}, **json.loads(r["payload"])))
        return corrections
