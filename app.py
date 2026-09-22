from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import Reconstructor, build_analysis, minimize_cycle
from store import Store

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")
store = Store(DB_PATH)

app = FastAPI(title="锁序解剖室")
app.mount("/static", StaticFiles(directory="static"), name="static")

_reconstructors: Dict[tuple, Reconstructor] = {}


def get_reconstructor(branch_id: str, policy: str = "inferred") -> Reconstructor:
    key = (branch_id, policy)
    if key not in _reconstructors:
        if not store.branch_exists(branch_id):
            raise HTTPException(404, f"unknown branch: {branch_id}")
        _reconstructors[key] = Reconstructor(
            store.list_events(branch_id), store.list_declarations(branch_id), policy=policy
        )
    return _reconstructors[key]


def invalidate(branch_id: str) -> None:
    for key in [key for key in _reconstructors if key[0] == branch_id]:
        del _reconstructors[key]


class EventsPayload(BaseModel):
    events: List[Dict[str, Any]]


class CorrectionPayload(BaseModel):
    kind: str
    event_id: Optional[str] = None
    patch: Optional[Dict[str, Any]] = None
    declaration: Optional[Dict[str, Any]] = None
    name: Optional[str] = None


class MinimizePayload(BaseModel):
    cycle_id: str
    policy: str = "inferred"


@app.get("/")
def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/api/branches")
def branches() -> Dict[str, Any]:
    return {"branches": store.list_branches()}


@app.post("/api/branches/{branch_id}/events")
def add_events(branch_id: str, payload: EventsPayload) -> Dict[str, Any]:
    if not store.branch_exists(branch_id):
        raise HTTPException(404, f"unknown branch: {branch_id}")
    events = store.add_events(branch_id, payload.events)
    affected_from = None
    for policy in ("inferred", "strict"):
        key = (branch_id, policy)
        if key in _reconstructors:
            affected_from, _ = _reconstructors[key].append(events)
    return {
        "added": [event.as_dict() for event in events],
        "affected_from_ts": affected_from,
        "incremental": affected_from is not None,
    }


@app.get("/api/branches/{branch_id}/events")
def list_events(branch_id: str) -> Dict[str, Any]:
    if not store.branch_exists(branch_id):
        raise HTTPException(404, f"unknown branch: {branch_id}")
    return {"events": [event.as_dict() for event in store.list_events(branch_id)]}


@app.post("/api/branches/{branch_id}/corrections")
def correct(branch_id: str, payload: CorrectionPayload) -> Dict[str, Any]:
    if not store.branch_exists(branch_id):
        raise HTTPException(404, f"unknown branch: {branch_id}")
    name = payload.name or f"correction-of-{branch_id}"
    new_branch = store.clone_branch(branch_id, name)
    if payload.kind == "event":
        if not payload.event_id or payload.patch is None:
            raise HTTPException(400, "event correction requires event_id and patch")
        events = store.list_events(new_branch)
        target = next((event for event in events if event.id == payload.event_id), None)
        if target is None:
            raise HTTPException(404, f"unknown event: {payload.event_id}")
        raw = dict(target.raw)
        raw.update(payload.patch)
        raw["id"] = payload.event_id
        raw["seq"] = target.seq
        raw["source_event_id"] = payload.event_id
        store.update_event(new_branch, payload.event_id, raw)
    elif payload.kind == "declaration":
        if not payload.declaration:
            raise HTTPException(400, "declaration correction requires declaration")
        store.set_declaration(new_branch, payload.declaration)
    else:
        raise HTTPException(400, f"unknown correction kind: {payload.kind}")
    return {"branch_id": new_branch, "branches": store.list_branches()}


@app.get("/api/analysis")
def analysis(
    branch_id: str = Query(...),
    ts: Optional[float] = Query(None),
    policy: str = Query("inferred"),
) -> Dict[str, Any]:
    recon = get_reconstructor(branch_id, policy)
    state = recon.state_at(ts=ts)
    result = build_analysis(state, recon.events[: recon.index_at_ts(ts)], ts, recon.reordered)
    result["branch_id"] = branch_id
    result["policy"] = policy
    result["candidate_graphs"] = [
        {"policy": "inferred", "description": "候选重建：补入推断请求，保留置信状态"},
        {"policy": "strict", "description": "严格重建：不补任何缺失事件"},
    ]
    return result


@app.get("/api/compare")
def compare(
    left: str = Query(...),
    right: str = Query(...),
    ts: Optional[float] = Query(None),
) -> Dict[str, Any]:
    def summarize(branch_id: str) -> Dict[str, Any]:
        recon = get_reconstructor(branch_id)
        state = recon.state_at(ts=ts)
        result = build_analysis(state, recon.events[: recon.index_at_ts(ts)], ts, recon.reordered)
        return result

    left_view = summarize(left)
    right_view = summarize(right)
    left_cycles = {tuple(cycle["locks"]) for cycle in left_view["cycles"]}
    right_cycles = {tuple(cycle["locks"]) for cycle in right_view["cycles"]}
    return {
        "left": left_view,
        "right": right_view,
        "cycles_only_left": [list(item) for item in left_cycles - right_cycles],
        "cycles_only_right": [list(item) for item in right_cycles - left_cycles],
    }


@app.post("/api/branches/{branch_id}/minimize")
def minimize(branch_id: str, payload: MinimizePayload) -> Dict[str, Any]:
    if not store.branch_exists(branch_id):
        raise HTTPException(404, f"unknown branch: {branch_id}")
    try:
        return minimize_cycle(
            store.list_events(branch_id),
            store.list_declarations(branch_id),
            payload.cycle_id,
            policy=payload.policy,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc))


@app.on_event("startup")
def seed() -> None:
    if store.list_branches():
        return
    branch = store.create_branch("demo-deadlock")
    store.set_declaration(branch, {"lock": "0xA", "lock_type": "reentrant"})
    store.set_declaration(branch, {"lock": "0xB", "lock_type": "reentrant"})
    store.add_events(
        branch,
        [
            {"id": "e1", "kind": "thread_start", "thread": "T1", "ts": 1},
            {"id": "e2", "kind": "thread_start", "thread": "T2", "ts": 1},
            {"id": "e3", "kind": "acquire_request", "thread": "T1", "lock": "0xA", "ts": 2,
             "stack": ["T1.acquire(0xA)", "worker.run"]},
            {"id": "e4", "kind": "acquire_success", "thread": "T1", "lock": "0xA", "ts": 3,
             "stack": ["T1.acquire(0xA)", "worker.run"]},
            {"id": "e5", "kind": "acquire_request", "thread": "T2", "lock": "0xB", "ts": 4,
             "stack": ["T2.acquire(0xB)", "worker.run"]},
            {"id": "e6", "kind": "acquire_success", "thread": "T2", "lock": "0xB", "ts": 5,
             "stack": ["T2.acquire(0xB)", "worker.run"]},
            {"id": "e7", "kind": "acquire_request", "thread": "T1", "lock": "0xB", "ts": 6,
             "stack": ["T1.acquire(0xB)", "worker.run"]},
            {"id": "e8", "kind": "acquire_request", "thread": "T2", "lock": "0xA", "ts": 7,
             "stack": ["T2.acquire(0xA)", "worker.run"]},
        ],
    )
