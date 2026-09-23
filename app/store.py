"""SQLite persistence for events, correction branches and replay checkpoints."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any

from .engine import (analyze, blocked_edges, cond_edges, detect_cycles,
                    hold_view, lock_type_view, new_state, ordered_events,
                    reduce_events, replay)

CHECKPOINT_EVENTS = 25

SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER,
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    gen TEXT NOT NULL,
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    thread TEXT,
    lock TEXT,
    cond TEXT,
    mode TEXT,
    timeout_ms INTEGER,
    all_waiters INTEGER,
    stack TEXT,
    payload TEXT,
    arrival_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS lock_types (
    branch_id INTEGER NOT NULL,
    lock TEXT NOT NULL,
    ltype TEXT NOT NULL,
    PRIMARY KEY (branch_id, lock)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    branch_id INTEGER NOT NULL,
    after_event_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (branch_id, after_event_id)
);
"""


def _row_from_payload(seq, arrival_seq, data):
    stack = data.get("stack")
    if isinstance(stack, list):
        stack = "\n".join(str(x) for x in stack)
    return {
        "seq": seq,
        "gen": str(data.get("gen", "g0")),
        "ts": int(data.get("ts", 0)),
        "kind": data["kind"],
        "thread": data.get("thread"),
        "lock": data.get("lock"),
        "cond": data.get("cond"),
        "mode": data.get("mode"),
        "timeout_ms": data.get("timeout_ms"),
        "all_waiters": 1 if data.get("all") else None,
        "stack": stack,
        "extra": {
            k: v for k, v in data.items()
            if k not in {"seq", "gen", "ts", "kind", "thread", "lock",
                         "cond", "mode", "timeout_ms", "all", "stack"}
        },
    }


def _event_to_dict(row) -> dict[str, Any]:
    extra = {}
    if row["payload"]:
        try:
            extra = json.loads(row["payload"])
        except json.JSONDecodeError:
            extra = {}
    out = {
        "id": row["id"],
        "seq": row["seq"],
        "gen": row["gen"],
        "ts": row["ts"],
        "kind": row["kind"],
        "thread": row["thread"],
        "lock": row["lock"],
        "cond": row["cond"],
        "mode": row["mode"] or "E",
        "timeout_ms": row["timeout_ms"],
        "all": bool(row["all_waiters"]),
        "stack": row["stack"],
        "arrival_seq": row["arrival_seq"],
    }
    out.update(extra)
    return out


class Store:
    def __init__(self, path: str = "data/locklab.db"):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.RLock()
        with self._conn() as conn:
            conn.executescript(SCHEMA)
            if conn.execute("SELECT id FROM branches WHERE id = 1").fetchone() is None:
                conn.execute(
                    "INSERT INTO branches (id, name, note) VALUES (1, '主干', 'initial branch')"
                )

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # branches -------------------------------------------------------------
    def list_branches(self):
        with self._lock, self._conn() as conn:
            return [dict(r) for r in
                    conn.execute("SELECT * FROM branches ORDER BY id")]

    def get_branch(self, branch_id):
        with self._lock, self._conn() as conn:
            row = conn.execute("SELECT * FROM branches WHERE id = ?",
                               (branch_id,)).fetchone()
            if row is None:
                raise KeyError(f"branch {branch_id}")
            return dict(row)

    def fork_branch(self, parent_id, name, note=""):
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO branches (name, parent_id, note) VALUES (?,?,?)",
                (name, parent_id, note))
            new_id = cur.lastrowid
            conn.execute(
                """INSERT INTO events
                   (branch_id, seq, gen, ts, kind, thread, lock, cond, mode,
                    timeout_ms, all_waiters, stack, payload, arrival_seq)
                   SELECT ?, seq, gen, ts, kind, thread, lock, cond, mode,
                          timeout_ms, all_waiters, stack, payload, arrival_seq
                   FROM events WHERE branch_id = ?""",
                (new_id, parent_id))
            conn.execute(
                "INSERT INTO lock_types (branch_id, lock, ltype) "
                "SELECT ?, lock, ltype FROM lock_types WHERE branch_id = ?",
                (new_id, parent_id))
            return new_id

    # events ---------------------------------------------------------------
    def _invalidate_after(self, conn, branch_id, seq):
        conn.execute(
            """DELETE FROM checkpoints WHERE branch_id = ?
               AND after_event_id IN (
                   SELECT id FROM events WHERE branch_id = ? AND seq >= ?)""",
            (branch_id, branch_id, seq))

    def add_event(self, branch_id, data):
        with self._lock, self._conn() as conn:
            nxt = conn.execute(
                "SELECT COALESCE(MAX(arrival_seq), 0) + 1 FROM events WHERE branch_id = ?",
                (branch_id,)).fetchone()[0]
            max_seq = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM events WHERE branch_id = ? AND gen = ?",
                (branch_id, str(data.get("gen", "g0")))).fetchone()[0]
            seq = data.get("seq")
            if seq is None:
                seq = max_seq + 1
            p = _row_from_payload(seq, nxt, data)
            conn.execute(
                """INSERT INTO events
                   (branch_id, seq, gen, ts, kind, thread, lock, cond, mode,
                    timeout_ms, all_waiters, stack, payload, arrival_seq)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (branch_id, p["seq"], p["gen"], p["ts"], p["kind"], p["thread"],
                 p["lock"], p["cond"], p["mode"], p["timeout_ms"],
                 p["all_waiters"], p["stack"],
                 json.dumps(p["extra"], ensure_ascii=False), nxt))
            self._invalidate_after(conn, branch_id, seq)
            return self._last_inserted(conn, branch_id, nxt)

    def _last_inserted(self, conn, branch_id, arrival_seq):
        row = conn.execute(
            "SELECT * FROM events WHERE branch_id = ? AND arrival_seq = ?",
            (branch_id, arrival_seq)).fetchone()
        return _event_to_dict(row)

    def add_events(self, branch_id, items):
        return [self.add_event(branch_id, it) for it in items]

    def update_event(self, branch_id, event_id, patch):
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM events WHERE id = ? AND branch_id = ?",
                (event_id, branch_id)).fetchone()
            if row is None:
                raise KeyError(f"event {event_id}")
            data = _event_to_dict(row)
            for k, v in patch.items():
                if v is not None:
                    data[k] = v
            fields = ("ts", "kind", "thread", "lock", "cond", "mode",
                      "timeout_ms", "all", "stack", "seq", "gen")
            clean = {k: data[k] for k in fields if k in data}
            sets, vals = [], []
            for k, v in clean.items():
                col = "all_waiters" if k == "all" else k
                if k == "all":
                    v = 1 if v else None
                sets.append(f"{col} = ?")
                vals.append(v)
            conn.execute(
                f"UPDATE events SET {', '.join(sets)} WHERE id = ? AND branch_id = ?",
                vals + [event_id, branch_id])
            self._invalidate_after(
                conn, branch_id, min(row["seq"], int(clean.get("seq", row["seq"]))))
            return self.get_event(branch_id, event_id)

    def get_event(self, branch_id, event_id):
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM events WHERE id = ? AND branch_id = ?",
                (event_id, branch_id)).fetchone()
            if row is None:
                raise KeyError(f"event {event_id}")
            return _event_to_dict(row)

    def delete_event(self, branch_id, event_id):
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT seq FROM events WHERE id = ? AND branch_id = ?",
                (event_id, branch_id)).fetchone()
            if row is None:
                raise KeyError(f"event {event_id}")
            conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
            self._invalidate_after(conn, branch_id, row["seq"])

    def get_events(self, branch_id):
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM events WHERE branch_id = ? ORDER BY arrival_seq",
                (branch_id,)).fetchall()
            return [_event_to_dict(r) for r in rows]

    def declared_types(self, branch_id):
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                "SELECT lock, ltype FROM lock_types WHERE branch_id = ?",
                (branch_id,)).fetchall()
            return {r["lock"]: r["ltype"] for r in rows}

    def declare_lock_type(self, branch_id, lock, ltype):
        if ltype not in ("mutex", "recursive", "rwlock"):
            raise ValueError(f"invalid lock type {ltype}")
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO lock_types (branch_id, lock, ltype) VALUES (?,?,?)
                   ON CONFLICT(branch_id, lock)
                   DO UPDATE SET ltype = excluded.ltype""",
                (branch_id, lock, ltype))
            conn.execute("DELETE FROM checkpoints WHERE branch_id = ?", (branch_id,))

    def reset(self):
        with self._lock, self._conn() as conn:
            for table in ("events", "lock_types", "checkpoints", "branches"):
                conn.execute(f"DELETE FROM {table}")
            conn.execute(
                "INSERT INTO branches (id, name, note) VALUES (1, '主干', 'initial branch')")

    # analytics ------------------------------------------------------------
    @staticmethod
    def _serialize_state(state):
        return {
            "gen": state["gen"],
            "locks": state["locks"],
            "threads": state["threads"],
            "waiters": state["waiters"],
            "next_waiter": state["next_waiter"],
            "next_ep": state["next_ep"],
            "anomalies": state["anomalies"],
            "ep_by_event": {str(k): v for k, v in state["ep_by_event"].items()},
            "links": state["links"],
            "processed": state["processed"],
            "last_event_id": state["last_event_id"],
            "last_ts": state["last_ts"],
        }

    @staticmethod
    def _deserialize_state(data):
        st = new_state()
        st["gen"] = data["gen"]
        st["locks"] = data["locks"]
        st["threads"] = data["threads"]
        st["waiters"] = {int(k): v for k, v in data["waiters"].items()}
        st["next_waiter"] = data["next_waiter"]
        st["next_ep"] = data["next_ep"]
        st["anomalies"] = data["anomalies"]
        st["ep_by_event"] = {int(k): v for k, v in data["ep_by_event"].items()}
        st["links"] = [tuple(x) for x in data["links"]]
        st["processed"] = data["processed"]
        st["last_event_id"] = data["last_event_id"]
        st["last_ts"] = data["last_ts"]
        return st

    def _replay_to_end(self, conn, branch_id):
        """Replay up to the last event, resuming from the newest checkpoint."""
        all_events = ordered_events(self.get_events(branch_id))
        if not all_events:
            return new_state(), []
        row = conn.execute(
            """SELECT after_event_id, state FROM checkpoints
               WHERE branch_id = ? ORDER BY after_event_id DESC LIMIT 1""",
            (branch_id,)).fetchone()
        if row is None:
            state, start = new_state(), 0
        else:
            ids = {e["id"] for e in all_events}
            if row["after_event_id"] not in ids:
                state, start = new_state(), 0
            else:
                state = self._deserialize_state(json.loads(row["state"]))
                start = next(i for i, e in enumerate(all_events)
                             if e["id"] == row["after_event_id"]) + 1
        declared = self.declared_types(branch_id)
        saved_to = start - 1
        for i in range(start, len(all_events)):
            replay([all_events[i]], declared, state)
            count = i + 1
            if count % CHECKPOINT_EVENTS == 0 and count < len(all_events):
                conn.execute(
                    """INSERT INTO checkpoints (branch_id, after_event_id, state)
                       VALUES (?,?,?)
                       ON CONFLICT(branch_id, after_event_id)
                       DO UPDATE SET state = excluded.state""",
                    (branch_id, all_events[i]["id"],
                     json.dumps(self._serialize_state(state), ensure_ascii=False)))
                saved_to = i
        return state, all_events

    def _report(self, st, window_count, last_event):
        edges = blocked_edges(st)
        cycles = detect_cycles(edges)
        return {
            "hold_graph": hold_view(st),
            "wait_graph": edges,
            "cond_graph": cond_edges(st),
            "cycles": cycles,
            "anomalies": st["anomalies"],
            "lock_types": lock_type_view(st),
            "events_included": window_count,
            "cutoff_event_id": last_event["id"] if last_event else None,
            "cutoff_ts": last_event["ts"] if last_event else None,
        }

    def analyze_at(self, branch_id, ts=None, event_id=None):
        """Incremental analysis: resume from a checkpoint and replay only the
        suffix affected by the requested time/event cutoff (and any events
        appended since the last checkpoint)."""
        with self._lock, self._conn() as conn:
            declared = self.declared_types(branch_id)
            all_events = ordered_events(self.get_events(branch_id))
            # Materialize/check checkpoints; this itself replays only the
            # suffix after the newest still-valid checkpoint.
            self._replay_to_end(conn, branch_id)
            if not all_events:
                report = self._report(new_state(), 0, None)
            else:
                if event_id is not None:
                    cut = next((i for i, e in enumerate(all_events)
                                if e["id"] == event_id), len(all_events) - 1)
                elif ts is None:
                    cut = len(all_events) - 1
                else:
                    cut = max((i for i, e in enumerate(all_events)
                               if e["ts"] <= ts), default=-1)
                if cut < 0:
                    report = self._report(new_state(), 0, None)
                else:
                    cp_rows = conn.execute(
                        """SELECT after_event_id, state FROM checkpoints
                           WHERE branch_id = ?""", (branch_id,)).fetchall()
                    cp_by_id = {r["after_event_id"]: r for r in cp_rows}
                    chosen = None
                    for i in range(cut, -1, -1):
                        if all_events[i]["id"] in cp_by_id:
                            chosen = i
                            break
                    if chosen is None:
                        state, start = new_state(), 0
                    else:
                        state = self._deserialize_state(
                            json.loads(cp_by_id[all_events[chosen]["id"]]["state"]))
                        start = chosen + 1
                    for i in range(start, cut + 1):
                        replay([all_events[i]], declared, state)
                    report = self._report(state, cut + 1, all_events[cut])
            report["branch_id"] = branch_id
            report["total_events"] = len(all_events)
            return report

    def reduce(self, branch_id, fingerprint=None, ts=None, event_id=None,
               create_branch=False, name=None):
        with self._lock:
            events = self.get_events(branch_id)
            declared = self.declared_types(branch_id)
            result = reduce_events(events, declared, fingerprint=fingerprint,
                                   ts=ts, event_id=event_id)
            if create_branch and result["reason"] == "ok":
                new_id = self.fork_branch(
                    branch_id,
                    name or f"最小片段@{branch_id}",
                    f"reduced from {result['original_count']} to "
                    f"{len(result['reduced'])} events")
                with self._conn() as conn:
                    conn.execute("DELETE FROM events WHERE branch_id = ?", (new_id,))
                stripped = [{k: v for k, v in e.items()
                             if k not in {"id", "arrival_seq"}}
                            for e in result["reduced"]]
                inserted = self.add_events(new_id, stripped)
                result["new_branch_id"] = new_id
                result["reduced"] = inserted
            return result
