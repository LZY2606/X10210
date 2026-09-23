"""锁序解剖室 FastAPI 入口。"""
from __future__ import annotations

import json
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import engine, samples, store

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")
HERE = os.path.dirname(__file__)

app = FastAPI(title="锁序解剖室", version="1.0.0")

conn = store.connect(DB_PATH)
store.init_db(conn)

# 流式重放缓存：(run_id, branch_id) -> Replayer + 已应用条数
_replay_cache: dict[tuple, dict] = {}


# ---------------------------------------------------------------------------
# 模型
# ---------------------------------------------------------------------------

class CreateRunIn(BaseModel):
    name: str = "未命名轨迹"
    note: str = ""
    sample: Optional[str] = None


class EventIn(BaseModel):
    type: str
    thread: str
    ts: float = 0.0
    gen: str = "g0"
    lock: Optional[str] = None
    mode: str = "write"
    cond: Optional[str] = None
    notify_all: bool = False
    seq: Optional[int] = None
    stack: list[str] = Field(default_factory=list)


class EventBatchIn(BaseModel):
    events: list[EventIn]


class BranchIn(BaseModel):
    parent_id: str = "main"
    label: str = "校正分支"
    correction: dict = Field(default_factory=dict)


class LockTypeIn(BaseModel):
    gen: str
    lock: str
    type: str  # mutex / rwlock
    branch_id: str = "main"


class CompareIn(BaseModel):
    branch_a: str = "main"
    branch_b: str


class ReduceIn(BaseModel):
    branch_id: str = "main"
    target_signature: Optional[str] = None


class StateIn(BaseModel):
    branch_id: str = "main"
    index: Optional[int] = None
    candidate: int = 0


# ---------------------------------------------------------------------------
# 帮助
# ---------------------------------------------------------------------------

def _require_run(run_id: str):
    run = store.get_run(conn, run_id)
    if not run:
        raise HTTPException(404, f"run {run_id} 不存在")
    return run


def _events_for_branch(run_id: str, branch_id: str):
    branches = {b["id"] for b in store.list_branches(conn, run_id)}
    if branch_id not in branches:
        raise HTTPException(404, f"分支 {branch_id} 不存在")
    base_events = store.load_events(conn, run_id, branch_id)
    return base_events


def _invalidate_cache(run_id: str, branch_id: Optional[str] = None):
    for key in list(_replay_cache):
        if key[0] == run_id and (branch_id is None or key[1] == branch_id):
            _replay_cache.pop(key, None)


def _effective_events(run_id: str, branch_id: str):
    branches = {b["id"]: b for b in store.list_branches(conn, run_id)}
    base = _events_for_branch(run_id, branch_id)
    correction = branches.get(branch_id, {}).get("correction", {})
    return engine.build_effective(base, correction)


def _candidate_result(run_id: str, branch_id: str, index: Optional[int]):
    events = _effective_events(run_id, branch_id)
    declared = store.declared_types(conn, run_id, branch_id)
    return engine.replay_candidates(events, index=index, declared_types=declared)


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/api/health")
def health():
    return {"ok": True, "title": "锁序解剖室"}


@app.get("/api/samples")
def get_samples():
    return {key: {"label": label} for key, (label, _fn) in samples.SAMPLES.items()}


@app.post("/api/runs")
def create_run(body: CreateRunIn):
    run = store.create_run(conn, body.name, body.note)
    loaded = []
    if body.sample:
        if body.sample not in samples.SAMPLES:
            raise HTTPException(400, f"未知样例 {body.sample}")
        _label, fn = samples.SAMPLES[body.sample]
        ids = store.insert_events_bulk(conn, run["id"], fn(), "main")
        loaded = ids
    return {**run, "loaded_events": len(loaded)}


@app.get("/api/runs")
def get_runs():
    return store.list_runs(conn)


@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    run = _require_run(run_id)
    run["branches"] = store.list_branches(conn, run_id)
    events = store.load_events(conn, run_id, "main")
    run["event_count"] = len(events)
    return run


@app.get("/api/runs/{run_id}/branches/{branch_id}/events")
def get_events(run_id: str, branch_id: str):
    _require_run(run_id)
    events = _effective_events(run_id, branch_id)
    return {
        "events": [ev.to_public() for ev in engine.ordered(events)],
        "count": len(events),
    }


@app.post("/api/runs/{run_id}/branches/{branch_id}/events")
async def append_events(run_id: str, branch_id: str, request: Request):
    """流式追加单条或多条事件；只重算受影响时间段。"""
    _require_run(run_id)
    _events_for_branch(run_id, branch_id)
    payload = await request.json()
    if isinstance(payload, dict) and "events" in payload:
        raws = payload["events"]
    elif isinstance(payload, list):
        raws = payload
    else:
        raws = [payload]
    validated = [EventIn(**r).model_dump() for r in raws]
    before = len(_effective_events(run_id, branch_id))
    prior_max_ts = max(
        (e.ts for e in _effective_events(run_id, branch_id)), default=None
    )
    ids = store.insert_events_bulk(conn, run_id, validated, branch_id)
    after = len(_effective_events(run_id, branch_id))
    affected_from = before
    if prior_max_ts is not None and any(float(r.get("ts", 0)) < prior_max_ts for r in validated):
        # 乱序到达：新增事件落在已重算时间段之前，整段重算
        _invalidate_cache(run_id, branch_id)
        affected_from = 0
    else:
        # 顺序流式追加：仅重算新增时间段
        _invalidate_cache(run_id, branch_id)
    return {
        "ids": ids,
        "appended": len(ids),
        "total": after,
        "recomputed_from_index": affected_from,
        "recomputed_span": [affected_from, after],
        "incremental": affected_from > 0,
    }


@app.get("/api/runs/{run_id}/branches/{branch_id}/state")
def get_state(run_id: str, branch_id: str, index: Optional[int] = None,
              candidate: int = 0):
    _require_run(run_id)
    result = _candidate_result(run_id, branch_id, index)
    n = candidate if 0 <= candidate < result["count"] else result["canonical_index"]
    chosen = result["candidates"][n]
    return {
        "canonical_index": result["canonical_index"],
        "selected": n,
        "ambiguous": result["ambiguous"],
        "candidate_count": result["count"],
        "state": chosen,
    }


@app.post("/api/runs/{run_id}/branches")
def add_branch(run_id: str, body: BranchIn):
    _require_run(run_id)
    created = store.create_branch(conn, run_id, body.parent_id, body.label, body.correction)
    return created


@app.get("/api/runs/{run_id}/branches")
def get_branches(run_id: str):
    _require_run(run_id)
    return store.list_branches(conn, run_id)


@app.post("/api/runs/{run_id}/branches/{branch_id}/lock-type")
def declare_lock_type(run_id: str, branch_id: str, body: LockTypeIn):
    _require_run(run_id)
    if body.type not in (engine.MUTEX, engine.RWLOCK):
        raise HTTPException(400, "锁类型必须是 mutex 或 rwlock")
    store.set_lock_type(conn, run_id, branch_id, body.gen, body.lock, body.type)
    _invalidate_cache(run_id, branch_id)
    return {"ok": True, f"{body.gen}::{body.lock}": body.type}


@app.post("/api/runs/{run_id}/compare")
def compare_branches(run_id: str, body: CompareIn):
    _require_run(run_id)
    ra = _candidate_result(run_id, body.branch_a, None)
    rb = _candidate_result(run_id, body.branch_b, None)
    sa = ra["candidates"][ra["canonical_index"]]
    sb = rb["candidates"][rb["canonical_index"]]
    ea = _effective_events(run_id, body.branch_a)
    eb = _effective_events(run_id, body.branch_b)
    ids_a = {e.id for e in ea}
    ids_b = {e.id for e in eb}
    return {
        "branch_a": body.branch_a,
        "branch_b": body.branch_b,
        "events_only_in_a": sorted(i for i in ids_a - ids_b if i is not None),
        "events_only_in_b": sorted(i for i in ids_b - ids_a if i is not None),
        "confidence": {"a": sa["confidence"], "b": sb["confidence"]},
        "anomaly_count": {"a": len(sa["anomalies"]), "b": len(sb["anomalies"])},
        "cycles_a": [c["signature"] for c in sa["cycles"]],
        "cycles_b": [c["signature"] for c in sb["cycles"]],
        "cycle_sets_equal": {c["signature"] for c in sa["cycles"]}
        == {c["signature"] for c in sb["cycles"]},
        "anomalies_a": sa["anomalies"],
        "anomalies_b": sb["anomalies"],
    }


@app.post("/api/runs/{run_id}/reduce")
def reduce_trace(run_id: str, body: ReduceIn):
    _require_run(run_id)
    events = _effective_events(run_id, body.branch_id)
    declared = store.declared_types(conn, run_id, body.branch_id)
    result = engine.minimize_events(events, body.target_signature, declared)
    return result


app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")
