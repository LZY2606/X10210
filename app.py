"""锁序解剖室 — FastAPI 入口。

运行：.venv/bin/uvicorn app:app --host 127.0.0.1 --port 5270
"""
from __future__ import annotations

import os
import threading

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from locklab import db
from locklab.engine import Incremental, LOCK_TYPES, replay
from locklab.minimize import minimize

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")

app = FastAPI(title="锁序解剖室")
_conn = db.connect(DB_PATH)
_lock = threading.Lock()
_main = db.ensure_main(_conn)
_streams = {}  # branch -> Incremental（流式增量重建缓存）

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _branch_or_404(branch):
    row = _conn.execute("SELECT id FROM branches WHERE id=?", (branch,)).fetchone()
    if not row:
        raise HTTPException(404, f"branch {branch} not found")


def _snapshot(branch, t=None):
    events = db.list_events(_conn, branch)
    decls = db.get_declarations(_conn, branch)
    return replay(events, t=t, declarations=decls)


class EventIn(BaseModel):
    type: str
    ts: float = 0
    thread: str | None = None
    lock: str | None = None
    kind: str | None = None
    wait_id: str | None = None
    timeout_ms: float | None = None
    cond: str | None = None
    all: bool | None = None


class ImportBody(BaseModel):
    branch: int | None = None
    events: list[EventIn]


class Correction(BaseModel):
    event_id: int | None = None
    patch: dict | None = None
    new_event: dict | None = None


class DeclarationIn(BaseModel):
    epoch: int
    lock: str
    ltype: str


class BranchBody(BaseModel):
    name: str
    parent: int | None = None
    corrections: list[Correction] = []
    declarations: list[DeclarationIn] = []
    note: str = ""


class MinimizeBody(BaseModel):
    branch: int
    threads: list[str] | None = None
    cycle_id: int | None = None
    t: float | None = None


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/branches")
def branches():
    with _lock:
        return {"branches": db.list_branches(_conn), "main": _main}


@app.post("/api/branches")
def new_branch(body: BranchBody):
    with _lock:
        parent = body.parent if body.parent is not None else _main
        _branch_or_404(parent)
        for d in body.declarations:
            if d.ltype not in LOCK_TYPES:
                raise HTTPException(400, f"unknown lock type {d.ltype}")
        branch = db.create_branch(
            _conn, body.name, parent,
            corrections=[c.model_dump(exclude_none=True) for c in body.corrections],
            declarations=[d.model_dump() for d in body.declarations],
            note=body.note)
        _streams.pop(branch, None)
        return {"branch": branch, "branches": db.list_branches(_conn)}


@app.post("/api/epochs/{epoch}/events")
def import_events(epoch: int, body: ImportBody):
    """按采集代次导入一批事件，保留批内原始顺序。"""
    with _lock:
        branch = body.branch or _main
        _branch_or_404(branch)
        ids = db.insert_events(_conn, branch, epoch, [e.model_dump() for e in body.events])
        _streams.pop(branch, None)  # 非流式导入使增量缓存失效
        return {"branch": branch, "epoch": epoch, "ids": ids}


@app.post("/api/stream")
def stream_events(body: ImportBody, epoch: int = 0):
    """流式追加：只重算受影响的尾段，返回重算统计。"""
    with _lock:
        branch = body.branch or _main
        _branch_or_404(branch)
        payloads = []
        for e in body.events:
            d = e.model_dump()
            d["epoch"] = epoch
            payloads.append(d)
        ids = db.insert_events(_conn, branch, epoch, payloads)
        stream = _streams.get(branch)
        if stream is None:
            stream = Incremental(db.get_declarations(_conn, branch))
            stream.append(db.list_events(_conn, branch))
        else:
            stored = db.list_events(_conn, branch)
            by_id = {e["id"]: e for e in stored}
            stream.append([by_id[i] for i in ids])
        _streams[branch] = stream
        return {"branch": branch, "ids": ids, "stats": dict(stream.stats)}


@app.get("/api/graph")
def graph(branch: int, t: float | None = None):
    with _lock:
        _branch_or_404(branch)
        return _snapshot(branch, t)


@app.get("/api/timeline")
def timeline(branch: int):
    with _lock:
        _branch_or_404(branch)
        events = db.list_events(_conn, branch)
        threads = sorted({e["thread"] for e in events if e.get("thread")})
        ts = [e["ts"] for e in events]
        return {
            "threads": threads,
            "events": events,
            "t_min": min(ts) if ts else 0,
            "t_max": max(ts) if ts else 0,
            "declarations": [
                {"epoch": k[0], "lock": k[1], "ltype": v}
                for k, v in db.get_declarations(_conn, branch).items()],
        }


@app.get("/api/compare")
def compare(a: int, b: int, t: float | None = None):
    """比较两个分支：事件差异 + 同一时点的图差异。"""
    with _lock:
        _branch_or_404(a)
        _branch_or_404(b)
        ea = {(e["epoch"], e["seq"]): e for e in db.list_events(_conn, a)}
        eb = {(e["epoch"], e["seq"]): e for e in db.list_events(_conn, b)}
        keys = sorted(set(ea) | set(eb))
        event_diff = []
        for k in keys:
            x, y = ea.get(k), eb.get(k)
            if x is None or y is None:
                event_diff.append({"key": k, "a": x, "b": y})
            else:
                changed = {f: (x.get(f), y.get(f)) for f in
                           ("type", "ts", "thread", "lock", "kind", "wait_id",
                            "timeout_ms", "cond")
                           if x.get(f) != y.get(f)}
                if changed:
                    event_diff.append({"key": k, "changed": changed, "a": x, "b": y})
        sa, sb = _snapshot(a, t), _snapshot(b, t)

        def edge_key(e):
            return (e["from"], e["to"], e["lock"])

        wa = {edge_key(e): e for e in sa["wait_for"]}
        wb = {edge_key(e): e for e in sb["wait_for"]}
        return {
            "event_diff": event_diff,
            "wait_only_a": [wa[k] for k in wa.keys() - wb.keys()],
            "wait_only_b": [wb[k] for k in wb.keys() - wa.keys()],
            "cycles_a": sa["cycles"],
            "cycles_b": sb["cycles"],
        }


@app.post("/api/minimize")
def minimize_endpoint(body: MinimizeBody):
    """把长轨迹缩减成仍复现同一环的最小事件片段。"""
    with _lock:
        _branch_or_404(body.branch)
        events = db.list_events(_conn, body.branch)
        decls = db.get_declarations(_conn, body.branch)
        if body.t is not None:
            events = [e for e in events if e["ts"] <= body.t]
        threads = body.threads
        if threads is None:
            snap = replay(events, declarations=decls)
            if not snap["cycles"]:
                raise HTTPException(400, "no cycle to minimize")
            idx = body.cycle_id or 0
            threads = snap["cycles"][idx]["threads"]
        reduced = minimize(events, threads, declarations=decls)
        return {"threads": threads, "original": len(events),
                "reduced": len(reduced), "events": reduced}


@app.post("/api/declarations")
def declare(body: DeclarationIn, branch: int | None = None):
    with _lock:
        b = branch or _main
        _branch_or_404(b)
        if body.ltype not in LOCK_TYPES:
            raise HTTPException(400, f"unknown lock type {body.ltype}")
        db.add_declaration(_conn, b, body.epoch, body.lock, body.ltype)
        _streams.pop(b, None)
        return {"ok": True}


@app.post("/api/demo")
def demo():
    """载入示例：AB-BA 死锁 + condition 交互 + 跨代次地址复用。"""
    with _lock:
        branch = _main
        demo_events = [
            # epoch 1：T1/T2 经典 AB-BA 死锁
            (1, "thread_start", 0, "T1", None, None, None, None, None),
            (1, "thread_start", 0, "T2", None, None, None, None, None),
            (1, "acquire_request", 1, "T1", "A", None, "w1", None, None),
            (1, "acquire", 2, "T1", "A", None, "w1", None, None),
            (1, "acquire_request", 3, "T2", "B", None, "w2", None, None),
            (1, "acquire", 4, "T2", "B", None, "w2", None, None),
            (1, "acquire_request", 5, "T1", "B", None, "w3", None, None),
            (1, "acquire_request", 6, "T2", "A", None, "w4", None, None),
            # epoch 1：T3 等待条件变量，T4 持锁后 notify
            (1, "acquire_request", 7, "T3", "M", None, "w5", None, None),
            (1, "acquire", 8, "T3", "M", None, "w5", None, None),
            (1, "cond_wait", 9, "T3", "M", None, None, None, "C"),
            (1, "acquire_request", 10, "T4", "M", None, "w6", None, None),
            (1, "acquire", 11, "T4", "M", None, "w6", None, None),
            (1, "cond_notify", 12, "T4", None, None, None, None, "C"),
            (1, "release", 13, "T4", "M", None, None, None, None),
            # epoch 2：进程重启，地址 A 复用为不同的锁，不构成环
            (2, "thread_start", 20, "U1", None, None, None, None, None),
            (2, "acquire_request", 21, "U1", "A", None, "w7", None, None),
            (2, "acquire", 22, "U1", "A", None, "w7", None, None),
            (2, "acquire_request", 23, "U2", "A", None, "w8", 50, None),
            (2, "thread_end", 200, "U2", None, None, None, None, None),
        ]
        payloads = [
            {"type": ty, "ts": ts, "thread": th, "lock": lk, "kind": kd,
             "wait_id": wi, "timeout_ms": to, "cond": cd}
            for (ep, ty, ts, th, lk, kd, wi, to, cd) in demo_events
        ]
        by_epoch = {}
        for (ep, *_), p in zip(demo_events, payloads):
            by_epoch.setdefault(ep, []).append(p)
        for ep, evs in by_epoch.items():
            db.insert_events(_conn, branch, ep, evs)
        _streams.pop(branch, None)
        return {"ok": True, "events": sum(len(v) for v in by_epoch.values())}
