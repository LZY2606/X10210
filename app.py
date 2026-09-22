from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from locklab.db import (
    append_events,
    connect,
    correct_event,
    get_branch,
    init_db,
    list_branches,
    load_events,
    load_lock_types,
    set_lock_type,
    clone_branch,
)
from locklab.engine import analyze
from locklab.reduction import reduce_cycle


app = FastAPI(title="锁序解剖室", version="1.0.0")
app.mount("/static", StaticFiles(directory=Path(__file__).with_name("static")), name="static")
db = connect()
init_db(db)


class EventIn(BaseModel):
    id: Optional[str] = None
    generation: str = "default"
    timestamp: float
    kind: str
    thread: str
    lock: Optional[str] = None
    mode: str = "exclusive"
    stack: List[str] = Field(default_factory=list)
    request_id: Optional[str] = None
    cond: Optional[str] = None
    broadcast: bool = False


class AppendIn(BaseModel):
    events: List[Dict[str, Any]]


class LockTypeIn(BaseModel):
    generation: str = "default"
    lock: str
    lock_type: str


class CorrectionIn(BaseModel):
    event_id: str
    patch: Dict[str, Any]
    new_branch_id: str
    name: str


class BranchIn(BaseModel):
    new_branch_id: str
    name: str
    note: str = ""


def branch_analysis(branch_id: str) -> Dict[str, Any]:
    events = load_events(db, branch_id)
    return analyze(events, load_lock_types(db, branch_id))


@app.get("/")
def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static/index.html")


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"name": "锁序解剖室", "status": "ok"}


@app.get("/api/branches")
def branches() -> Dict[str, Any]:
    return {"branches": list_branches(db)}


@app.post("/api/branches/{branch_id}/clone")
def clone(branch_id: str, payload: BranchIn) -> Dict[str, Any]:
    try:
        return {"branch": clone_branch(db, branch_id, payload.new_branch_id, payload.name, payload.note)}
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error))


@app.get("/api/branches/{branch_id}/analysis")
def get_analysis(branch_id: str, at: Optional[float] = None, generation: Optional[str] = None) -> Dict[str, Any]:
    try:
        get_branch(db, branch_id)
        result = branch_analysis(branch_id)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    if generation is not None:
        result["events"] = [event for event in result["events"] if event["generation"] == generation]
        result["raw_order"] = [event for event in result["raw_order"] if event["generation"] == generation]
        result["snapshots"] = [item for item in result["snapshots"] if item["generation"] == generation]
    if at is not None and result["snapshots"]:
        eligible = [item for item in result["snapshots"] if item["timestamp"] <= at]
        if eligible:
            result["snapshot"] = eligible[-1]
        else:
            result["snapshot"] = None
    else:
        result["snapshot"] = result["snapshots"][-1] if result["snapshots"] else None
    return result


@app.post("/api/branches/{branch_id}/events")
def post_events(branch_id: str, payload: AppendIn) -> Dict[str, Any]:
    try:
        return append_events(db, branch_id, payload.events)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.put("/api/branches/{branch_id}/lock-types")
def put_lock_type(branch_id: str, payload: LockTypeIn) -> Dict[str, str]:
    try:
        set_lock_type(db, branch_id, payload.generation, payload.lock, payload.lock_type)
        return {"status": "ok"}
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/api/branches/{branch_id}/corrections")
def correction(branch_id: str, payload: CorrectionIn) -> Dict[str, Any]:
    try:
        branch = correct_event(db, branch_id, payload.event_id, payload.patch, payload.new_branch_id, payload.name)
        return {"branch": branch, "analysis": branch_analysis(payload.new_branch_id)}
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.get("/api/branches/{branch_id}/compare/{other_id}")
def compare(branch_id: str, other_id: str) -> Dict[str, Any]:
    try:
        left = branch_analysis(branch_id)
        right = branch_analysis(other_id)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    left_events = {event["id"]: event for event in left["events"]}
    right_events = {event["id"]: event for event in right["events"]}
    changed_ids = sorted(event_id for event_id in left_events.keys() & right_events.keys() if left_events[event_id] != right_events[event_id])
    return {
        "left": {"branch_id": branch_id, "cycles": left["snapshots"][-1]["cycles"] if left["snapshots"] else []},
        "right": {"branch_id": other_id, "cycles": right["snapshots"][-1]["cycles"] if right["snapshots"] else []},
        "only_left": sorted(left_events.keys() - right_events.keys()),
        "only_right": sorted(right_events.keys() - left_events.keys()),
        "changed": [{ "id": event_id, "left": left_events[event_id], "right": right_events[event_id]} for event_id in changed_ids],
        "confidence": {
            "left": left["snapshots"][-1]["confidence"] if left["snapshots"] else None,
            "right": right["snapshots"][-1]["confidence"] if right["snapshots"] else None,
        },
    }


@app.post("/api/branches/{branch_id}/reduce")
def reduce(branch_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = reduce_cycle(
            load_events(db, branch_id),
            load_lock_types(db, branch_id),
            payload.get("cycle_id"),
        )
        return result
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/api/analyze")
def adhoc_analyze(payload: Dict[str, Any]) -> Dict[str, Any]:
    types = {
        (item.get("generation", "default"), item["lock"]): item["lock_type"]
        for item in payload.get("lock_types", [])
    }
    try:
        return analyze(payload.get("events", []), types)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.get("/api/sample")
def sample() -> Dict[str, Any]:
    return {"events": sample_events()}


def sample_events() -> List[Dict[str, Any]]:
    return [
        {"id": "a1", "generation": "boot-1", "timestamp": 1, "seq": 0, "kind": "thread_start", "thread": "T1"},
        {"id": "a2", "generation": "boot-1", "timestamp": 2, "seq": 1, "kind": "thread_start", "thread": "T2"},
        {"id": "a3", "generation": "boot-1", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "T1", "lock": "L1", "stack": ["T1 wants L1", "job.alpha"]},
        {"id": "a4", "generation": "boot-1", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "T1", "lock": "L1", "stack": ["T1 holds L1", "job.alpha"]},
        {"id": "a5", "generation": "boot-1", "timestamp": 5, "seq": 4, "kind": "acquire_request", "thread": "T2", "lock": "L2", "stack": ["T2 wants L2", "job.beta"]},
        {"id": "a6", "generation": "boot-1", "timestamp": 6, "seq": 5, "kind": "acquire_success", "thread": "T2", "lock": "L2", "stack": ["T2 holds L2", "job.beta"]},
        {"id": "a7", "generation": "boot-1", "timestamp": 7, "seq": 6, "kind": "acquire_request", "thread": "T1", "lock": "L2", "stack": ["T1 holds L1 waits L2", "job.alpha.step"]},
        {"id": "a8", "generation": "boot-1", "timestamp": 8, "seq": 7, "kind": "acquire_request", "thread": "T2", "lock": "L1", "stack": ["T2 holds L2 waits L1", "job.beta.step"]},
    ]
