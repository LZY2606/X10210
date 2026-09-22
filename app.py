"""Lock-order dissection room - FastAPI entry point."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.events import EventError
from app.minimize import minimize
from app.replay import analyze
from app.sample_data import SAMPLE_EVENTS, SAMPLE_LOCK_TYPES
from app.store import Store, lock_key
from app.streaming import IncrementalReplay

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
DB_PATH = os.environ.get("LOCKLAB_DB", os.path.join(BASE_DIR, "locklab.db"))

app = FastAPI(title="锁序解剖室", version="1.0")
store = Store(DB_PATH)

# one streaming replay cache per branch; invalidated when branch grows
_streams: Dict[int, IncrementalReplay] = {}


def _stream(branch_id: int) -> IncrementalReplay:
    if branch_id not in _streams:
        events = store.get_events(branch_id)
        _streams[branch_id] = IncrementalReplay(
            events=events, lock_types=store.get_lock_types(branch_id))
    return _streams[branch_id]


def _seed_if_empty() -> None:
    if store.get_events(0):
        return
    store.add_events(SAMPLE_EVENTS, branch_id=0)
    for key, ltype in SAMPLE_LOCK_TYPES.items():
        gen = int(key.split(":", 1)[0][1:])
        lock = key.split(":", 1)[1]
        store.set_lock_type(0, lock, gen, ltype)
    _streams.clear()


_seed_if_empty()


# ------------------------------------------------------------------ schemas
class IngestBody(BaseModel):
    events: List[Dict[str, Any]]
    branch_id: int = 0


class StateQuery(BaseModel):
    branch_id: int = 0
    at_ts: Optional[float] = None
    complete: bool = False


class CorrectionBody(BaseModel):
    parent_id: int = 0
    name: str
    note: str = ""
    kind: str  # edit | insert | delete
    event_id: Optional[int] = None
    event: Optional[Dict[str, Any]] = None
    events: Optional[List[Dict[str, Any]]] = None


class DeclareTypeBody(BaseModel):
    parent_id: int = 0
    name: str
    note: str = ""
    lock: str
    gen: int = 0
    lock_type: str = Field(..., pattern="^(mutex|recursive|rwlock)$")


class MinimizeBody(BaseModel):
    branch_id: int = 0
    at_ts: Optional[float] = None
    save_branch: Optional[str] = None


class CompareBody(BaseModel):
    a: int
    b: int


# ------------------------------------------------------------------- routes
@app.get("/")
def index() -> Any:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "title": "锁序解剖室"}


@app.get("/api/branches")
def branches() -> Any:
    return {"branches": store.list_branches()}


@app.get("/api/events")
def events(branch_id: int = 0) -> Any:
    return {"events": [e.to_dict() for e in store.get_events(branch_id)],
            "lock_types": store.get_lock_types(branch_id)}


@app.post("/api/events")
def ingest(body: IngestBody) -> Any:
    try:
        count = store.add_events(body.events, branch_id=body.branch_id)
    except EventError as exc:
        raise HTTPException(400, str(exc))
    _streams.pop(body.branch_id, None)
    return {"added": count, "total": len(store.get_events(body.branch_id))}


@app.post("/api/stream/append")
def stream_append(body: IngestBody) -> Any:
    """Streaming ingestion: persist then incrementally replay only tail."""
    try:
        normalized = []
        existing = store.get_events(body.branch_id)
        next_seq = (max((e.seq for e in existing), default=-1) + 1)
        for offset, raw in enumerate(body.events):
            payload = dict(raw)
            payload.setdefault("seq", next_seq + offset)
            from app.events import normalize_event
            normalized.append(normalize_event(
                payload, int(payload["seq"])))
        count = store.add_events(body.events, branch_id=body.branch_id)
    except EventError as exc:
        raise HTTPException(400, str(exc))
    stream = _streams.get(body.branch_id)
    if stream is None:
        stream = _stream(body.branch_id)
        info = {"affected_from": None, "affected_to": None,
                "replayed": len(normalized), "total":
                len(store.get_events(body.branch_id))}
    else:
        info = stream.append(normalized)
    snap = stream.snapshot(complete=False)
    return {"added": count, "replay": info,
            "cycle_count": len(snap["cycles"]),
            "affected": {"from_ts": info["affected_from"],
                         "to_ts": info["affected_to"]}}


@app.get("/api/state")
def state(branch_id: int = 0, at_ts: Optional[float] = None,
          complete: bool = False) -> Any:
    events_list = store.get_events(branch_id)
    lock_types = store.get_lock_types(branch_id)
    snap = analyze(events_list, lock_types, at_ts=at_ts, complete=complete)
    snap["branch_id"] = branch_id
    snap["timeline"] = sorted({e.ts for e in events_list})
    return snap


@app.post("/api/corrections")
def corrections(body: CorrectionBody) -> Any:
    payload: Dict[str, Any] = {}
    if body.kind == "edit":
        if body.event_id is None or body.event is None:
            raise HTTPException(400, "edit requires event_id and event")
        payload = {"event_id": body.event_id}
        payload.update(body.event)
    elif body.kind == "insert":
        payload = {"events": body.events or
                   ([body.event] if body.event else [])}
    elif body.kind == "delete":
        if body.event_id is None:
            raise HTTPException(400, "delete requires event_id")
        payload = {"event_id": body.event_id}
    else:
        raise HTTPException(400, "kind must be edit|insert|delete")
    try:
        new_id, detail = store.correct_event(
            body.parent_id, body.kind, payload, body.name, body.note)
    except (EventError, KeyError) as exc:
        raise HTTPException(400, str(exc))
    return {"branch_id": new_id, "detail": detail}


@app.post("/api/declare-type")
def declare_type(body: DeclareTypeBody) -> Any:
    try:
        new_id, detail = store.declare_lock_type(
            body.parent_id, body.lock, body.gen, body.lock_type,
            body.name, body.note)
    except EventError as exc:
        raise HTTPException(400, str(exc))
    return {"branch_id": new_id, "detail": detail}


@app.post("/api/minimize")
def minimize_route(body: MinimizeBody) -> Any:
    events_list = store.get_events(body.branch_id)
    lock_types = store.get_lock_types(body.branch_id)
    subset, repro_ts, info = minimize(
        events_list, lock_types, target_ts=body.at_ts)
    result_branch: Optional[int] = None
    if body.save_branch:
        result_branch = store.create_correction_branch(
            body.branch_id, body.save_branch,
            "minimized reproduction @ ts=%s" % repro_ts)
        # replace branch event membership with minimized ids
        kept_ids = {e.id for e in subset}
        all_events = store.get_events(body.branch_id)
        store.conn.execute(
            "DELETE FROM branch_events WHERE branch_id=?",
            (result_branch,))
        for eid in kept_ids:
            if eid is not None:
                store.conn.execute(
                    "INSERT OR IGNORE INTO branch_events(branch_id,event_id)"
                    " VALUES (?,?)", (result_branch, eid))
        store.conn.commit()
    return {"events": [e.to_dict() for e in subset],
            "repro_ts": repro_ts, "info": info,
            "saved_branch_id": result_branch}


@app.post("/api/compare")
def compare(body: CompareBody) -> Any:
    try:
        return store.compare(body.a, body.b)
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(400, str(exc))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
