"""FastAPI entry point for the lock-order dissection room."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from core.minimizer import minimize
from core.models import Event, event_from_dict
from core.storage import Store
from core.timeline import Timeline

DB_PATH = os.environ.get("DISSECTION_DB", "dissection.db")

app = FastAPI(title="锁序解剖室", version="1.0")
store = Store(DB_PATH)


class ImportPayload(BaseModel):
    events: list[dict[str, Any]]


class EventPayload(BaseModel):
    event: dict[str, Any]


class LockTypePayload(BaseModel):
    key: str
    type: str
    branch_id: int = 0


class BranchPayload(BaseModel):
    name: str
    parent_id: int = 0
    note: str | None = None


class CorrectionPayload(BaseModel):
    op: str
    target_seq: int | None = None
    payload: dict[str, Any] = {}


class MinimizePayload(BaseModel):
    branch_id: int = 0
    cycle_id: str | None = None


def _timeline(branch_id: int) -> Timeline:
    events, lock_types = store.materialize(branch_id)
    return Timeline(events, lock_types=lock_types)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "title": "锁序解剖室"}


@app.get("/api/meta")
def meta() -> dict[str, Any]:
    return {"lock_types": store.get_meta("lock_types", {})}


@app.post("/api/import")
def import_events(payload: ImportPayload) -> dict[str, Any]:
    events = []
    for index, data in enumerate(payload.events):
        data = dict(data)
        data.setdefault("seq", index)
        data.setdefault("arrival", data["seq"])
        events.append(event_from_dict(data, seq=int(data["seq"])))
    store.replace_events(events)
    return {"imported": len(events)}


@app.post("/api/events")
def append_event(payload: EventPayload) -> dict[str, Any]:
    data = dict(payload.event)
    if data.get("seq") is None:
        data["seq"] = store.next_seq()
    data.setdefault("arrival", store.next_seq())
    event = event_from_dict(data)
    store.append_event(event)
    # Demonstrate bounded incremental replay on the main timeline.
    timeline = _timeline(0)
    replay_count = timeline.append(event)
    return {
        "seq": event.seq,
        "inserted_at": sum(1 for other in timeline.events if other.key() <= event.key()),
        "replayed": replay_count,
    }


@app.get("/api/events")
def list_events(branch_id: int = 0) -> dict[str, Any]:
    timeline = _timeline(branch_id)
    return {
        "events": [event.to_dict() for event in timeline.events],
        "branch_id": branch_id,
    }


@app.post("/api/lock-types")
def declare_lock_type(payload: LockTypePayload) -> dict[str, Any]:
    if payload.branch_id == 0:
        types = dict(store.get_meta("lock_types", {}))
        types[payload.key] = payload.type
        store.set_meta("lock_types", types)
        return {"lock_types": types}
    store.add_correction(
        payload.branch_id,
        "set_lock_type",
        {"key": payload.key, "type": payload.type},
        None,
    )
    return {"branch_id": payload.branch_id, "op": "set_lock_type"}


@app.get("/api/snapshot")
def snapshot(branch_id: int = 0, ts: float | None = None, gen: int | None = None) -> dict[str, Any]:
    timeline = _timeline(branch_id)
    result = timeline.snapshot_at(ts=ts, gen=gen)
    result["branch_id"] = branch_id
    return result


@app.get("/api/incidents")
def incidents(branch_id: int = 0) -> dict[str, Any]:
    timeline = _timeline(branch_id)
    return {"incidents": timeline.incidents(), "branch_id": branch_id}


@app.get("/api/branches")
def branches() -> dict[str, Any]:
    return {"branches": store.list_branches()}


@app.post("/api/branches")
def create_branch(payload: BranchPayload) -> dict[str, Any]:
    if store.get_branch(payload.parent_id) is None:
        raise HTTPException(404, "parent branch not found")
    branch_id = store.create_branch(payload.name, payload.parent_id, payload.note)
    return {"branch_id": branch_id}


@app.post("/api/branches/{branch_id}/corrections")
def add_correction(branch_id: int, payload: CorrectionPayload) -> dict[str, Any]:
    if branch_id == 0:
        raise HTTPException(400, "corrections must live on a child branch")
    if store.get_branch(branch_id) is None:
        raise HTTPException(404, "branch not found")
    allowed = {"edit_event", "delete_event", "insert_event", "set_lock_type"}
    if payload.op not in allowed:
        raise HTTPException(400, f"op must be one of {sorted(allowed)}")
    correction_id = store.add_correction(
        branch_id, payload.op, payload.payload, payload.target_seq
    )
    return {"correction_id": correction_id}


@app.get("/api/branches/{branch_id}")
def branch_detail(branch_id: int) -> dict[str, Any]:
    branch = store.get_branch(branch_id)
    if branch is None:
        raise HTTPException(404, "branch not found")
    return branch


@app.get("/api/compare")
def compare(a: int = 0, b: int = 1, ts: float | None = None) -> dict[str, Any]:
    timeline_a = _timeline(a)
    timeline_b = _timeline(b)
    snap_a = timeline_a.snapshot_at(ts=ts)
    snap_b = timeline_b.snapshot_at(ts=ts)
    sig_a = {tuple(cycle["nodes"]) for cycle in snap_a["cycles"]}
    sig_b = {tuple(cycle["nodes"]) for cycle in snap_b["cycles"]}
    events_a = {event.seq: event.to_dict() for event in timeline_a.events}
    events_b = {event.seq: event.to_dict() for event in timeline_b.events}
    changed = sorted(
        seq
        for seq in events_a.keys() | events_b.keys()
        if events_a.get(seq) != events_b.get(seq)
    )
    return {
        "a": {"branch_id": a, "cycles": snap_a["cycles"], "issues": snap_a["issues"]},
        "b": {"branch_id": b, "cycles": snap_b["cycles"], "issues": snap_b["issues"]},
        "only_in_a": [list(key) for key in sig_a - sig_b],
        "only_in_b": [list(key) for key in sig_b - sig_a],
        "changed_event_seqs": changed,
    }


@app.post("/api/minimize")
def minimize_trace(payload: MinimizePayload) -> dict[str, Any]:
    timeline = _timeline(payload.branch_id)
    snapshot = timeline.final_snapshot()
    if not snapshot["cycles"]:
        raise HTTPException(400, "no cycle to minimize in this branch")
    target = snapshot["cycles"][0]
    if payload.cycle_id is not None:
        for cycle in snapshot["cycles"]:
            if cycle["id"] == payload.cycle_id:
                target = cycle
                break
    reduced = minimize(
        timeline.events,
        target,
        lock_types=timeline.engine.lock_types,
    )
    return {
        "cycle_id": target["id"],
        "original_count": len(timeline.events),
        "reduced_count": len(reduced),
        "events": [event.to_dict() for event in reduced],
    }


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")
