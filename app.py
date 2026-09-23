"""锁序解剖室：锁序死锁与近失事件复盘服务。"""
import json
import os
import time

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

import db
import engine as eng
from minimize import minimize_trace

app = FastAPI(title="锁序解剖室")
engine = eng.Engine()

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


class GenerationIn(BaseModel):
    label: str = ""


@app.post("/api/generations")
def create_generation(body: GenerationIn):
    conn = db.get_db()
    cur = conn.execute(
        "INSERT INTO generations(label, created_at) VALUES(?, ?)",
        (body.label, time.time()))
    conn.commit()
    gid = cur.lastrowid
    conn.close()
    return {"id": gid, "label": body.label}


@app.get("/api/generations")
def list_generations():
    conn = db.get_db()
    rows = conn.execute(
        "SELECT g.id, g.label, g.created_at, COUNT(e.id) AS events "
        "FROM generations g LEFT JOIN events e ON e.generation_id = g.id "
        "GROUP BY g.id ORDER BY g.id").fetchall()
    conn.close()
    return {"generations": [dict(r) for r in rows]}


class EventsIn(BaseModel):
    events: list[dict]


@app.post("/api/generations/{gid}/events")
def append_events(gid: int, body: EventsIn):
    conn = db.get_db()
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE generation_id = ?",
        (gid,)).fetchone()
    seq = row["m"]
    ids = []
    for ev in body.events:
        seq += 1
        cur = conn.execute(
            "INSERT INTO events(generation_id, seq, ts, kind, thread, lock, extra) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (gid, seq, ev.get("ts"), ev.get("kind"), ev.get("thread"),
             ev.get("lock"), json.dumps(ev.get("extra") or {})))
        ids.append(cur.lastrowid)
    conn.commit()
    conn.close()
    return {"ids": ids}


def _row_to_event(r):
    return {"id": r["id"], "seq": r["seq"], "ts": r["ts"], "kind": r["kind"],
            "thread": r["thread"], "lock": r["lock"],
            "extra": json.loads(r["extra"] or "{}")}


def load_events(conn, gid):
    rows = conn.execute(
        "SELECT * FROM events WHERE generation_id = ? ORDER BY seq", (gid,)
    ).fetchall()
    return [_row_to_event(r) for r in rows]


def branch_chain(conn, branch_id):
    chain, bid, seen = [], branch_id, set()
    while bid is not None and bid not in seen:
        seen.add(bid)
        row = conn.execute("SELECT * FROM branches WHERE id = ?", (bid,)).fetchone()
        if row is None:
            break
        chain.append(row["id"])
        bid = row["parent_id"]
    return list(reversed(chain))


def load_branch_inputs(conn, branch_id, gid):
    """基础事件 + 分支链上的校正，得到该分支的合法重建输入。"""
    events = load_events(conn, gid)
    declared = {}
    chain = branch_chain(conn, branch_id) or [branch_id]
    marks = ",".join("?" * len(chain))
    rows = conn.execute(
        f"SELECT * FROM corrections WHERE branch_id IN ({marks}) "
        f"AND generation_id = ? ORDER BY id",
        (*chain, gid)).fetchall()
    for r in rows:
        payload = json.loads(r["payload"])
        ctype = r["ctype"]
        if ctype == "event_patch":
            for ev in events:
                if ev["id"] == payload.get("event_id"):
                    for k in ("kind", "thread", "lock", "ts", "extra"):
                        if k in (payload.get("patch") or {}):
                            ev[k] = payload["patch"][k]
        elif ctype == "event_drop":
            events = [e for e in events if e["id"] != payload.get("event_id")]
        elif ctype == "event_insert":
            newev = dict(payload.get("event") or {})
            newev["id"] = f"ins-{r['id']}"
            idx = len(events)
            after = payload.get("after_event")
            if after is not None:
                for i, e in enumerate(events):
                    if e["id"] == after:
                        idx = i + 1
                        break
            events.insert(idx, newev)
        elif ctype == "lock_type":
            declared[payload["lock"]] = payload["lock_type"]
    return events, declared


@app.get("/api/generations/{gid}/events")
def get_events(gid: int, branch_id: int | None = None):
    conn = db.get_db()
    if branch_id:
        events, _ = load_branch_inputs(conn, branch_id, gid)
    else:
        events = load_events(conn, gid)
    conn.close()
    return {"events": events}


@app.get("/api/branches")
def list_branches():
    conn = db.get_db()
    rows = conn.execute(
        "SELECT * FROM branches ORDER BY id").fetchall()
    conn.close()
    return {"branches": [dict(r) for r in rows]}


class CorrectionIn(BaseModel):
    branch_id: int
    generation_id: int
    ctype: str
    payload: dict
    name: str | None = None


@app.post("/api/corrections")
def create_correction(body: CorrectionIn):
    """提交一条事件校正或锁类型声明，形成新分支；旧分支保留。"""
    conn = db.get_db()
    name = body.name or f"fix-{body.ctype}-{int(time.time())}"
    cur = conn.execute(
        "INSERT INTO branches(name, parent_id, created_at) VALUES(?, ?, ?)",
        (name, body.branch_id, time.time()))
    bid = cur.lastrowid
    conn.execute(
        "INSERT INTO corrections(branch_id, generation_id, ctype, payload, created_at) "
        "VALUES(?, ?, ?, ?, ?)",
        (bid, body.generation_id, body.ctype, json.dumps(body.payload), time.time()))
    conn.commit()
    conn.close()
    return {"branch_id": bid, "name": name}


def _state_payload(branch_id, gid, upto):
    conn = db.get_db()
    events, declared = load_branch_inputs(conn, branch_id, gid)
    conn.close()
    key = (db.db_path(), branch_id, gid)
    st = engine.state_for(key, events, declared, upto)
    graphs = eng.build_graphs(st)
    cycles = eng.cycle_reports(st, graphs)
    threads = {t: {"alive": th["alive"], "waiting": th["waiting"], "cond": th["cond"]}
               for t, th in st["threads"].items()}
    return {
        "meta": {"events_total": len(events), "upto": len(events) if upto is None else upto,
                 "replayed_events": engine.last_replayed},
        "threads": threads,
        "held": graphs["held"],
        "edges": graphs["edges"],
        "cond_blocked": graphs["cond_blocked"],
        "cycles": cycles,
        "anomalies": st["anomalies"],
        "lock_types": {l: lk["type"] for l, lk in st["locks"].items()},
    }


@app.get("/api/state")
def get_state(branch_id: int = 1, generation_id: int = 1, upto: int | None = None):
    return _state_payload(branch_id, generation_id, upto)


def _sigset(cycles):
    return {tuple(tuple(x) for x in c["signature"]) for c in cycles}


@app.get("/api/compare")
def compare(branch_a: int, branch_b: int, generation_id: int, upto: int | None = None):
    sa = _state_payload(branch_a, generation_id, upto)
    sb = _state_payload(branch_b, generation_id, upto)
    held_a = {(h["thread"], h["lock"]) for h in sa["held"]}
    held_b = {(h["thread"], h["lock"]) for h in sb["held"]}
    wait_a = {(e["from"], e["lock"]) for e in sa["edges"]}
    wait_b = {(e["from"], e["lock"]) for e in sb["edges"]}
    cyc_a, cyc_b = _sigset(sa["cycles"]), _sigset(sb["cycles"])
    return {
        "a": {"branch": branch_a, "held": sa["held"], "edges": sa["edges"],
              "cycles": sa["cycles"], "anomalies": sa["anomalies"]},
        "b": {"branch": branch_b, "held": sb["held"], "edges": sb["edges"],
              "cycles": sb["cycles"], "anomalies": sb["anomalies"]},
        "held_only_a": sorted(list(x) for x in held_a - held_b),
        "held_only_b": sorted(list(x) for x in held_b - held_a),
        "waits_only_a": sorted(list(x) for x in wait_a - wait_b),
        "waits_only_b": sorted(list(x) for x in wait_b - wait_a),
        "cycles_only_a": sorted([list(map(list, s)) for s in cyc_a - cyc_b]),
        "cycles_only_b": sorted([list(map(list, s)) for s in cyc_b - cyc_a]),
    }


class MinimizeIn(BaseModel):
    branch_id: int
    generation_id: int
    cycle_index: int = 0


@app.post("/api/minimize")
def minimize(body: MinimizeIn):
    conn = db.get_db()
    events, declared = load_branch_inputs(conn, body.branch_id, body.generation_id)
    conn.close()
    st = eng.replay(events, declared)
    cycles = eng.cycle_reports(st, eng.build_graphs(st))
    if not cycles:
        return {"error": "no_cycle", "message": "当前轨迹没有可复现的环"}
    idx = min(body.cycle_index, len(cycles) - 1)
    target = tuple(tuple(x) for x in cycles[idx]["signature"])
    kept = minimize_trace(events, declared, target)
    st2 = eng.replay(kept, declared)
    cycles2 = eng.cycle_reports(st2, eng.build_graphs(st2))
    return {
        "original_count": len(events),
        "minimal_count": len(kept),
        "events": kept,
        "cycles": cycles2,
    }
