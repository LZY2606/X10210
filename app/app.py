"""FastAPI 入口：锁序解剖室。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db as database
from .analysis import (
    diff_branches,
    enumerate_candidates,
    minimize_events,
)
from .engine import Replay, order_events
from .models import EVENT_KINDS, LOCK_KINDS, Event
from .samples import SAMPLES

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = str(BASE_DIR / "lab.db")

app = FastAPI(title="锁序解剖室", version="1.0")
conn = database.connect(DB_PATH)
database.init_db(conn)


# ---------------------------------------------------------------------------
class EventIn(BaseModel):
    ts: int
    kind: str
    thread: str
    gen: str = "g1"
    seq: Optional[int] = None
    lock: Optional[str] = None
    mode: Optional[str] = None
    gen_from: Optional[str] = None
    timeout: Optional[int] = None
    stack: List[str] = Field(default_factory=list)


class IngestIn(BaseModel):
    events: List[EventIn]
    reset: bool = False


class LockTypeIn(BaseModel):
    gen: str = "g1"
    lock: str
    kind: str


class OverrideIn(BaseModel):
    action: str  # delete_event | mutate_event | insert_event
    event_id: Optional[int] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class BranchIn(BaseModel):
    name: str
    parent_id: Optional[int] = None
    note: str = ""
    overrides: List[OverrideIn] = Field(default_factory=list)


class BranchCompareIn(BaseModel):
    left_id: int
    right_id: int


class MinimizeIn(BaseModel):
    branch_id: Optional[int] = None
    candidate: Optional[int] = None
    cycle_index: int = 0


def _validate_event(item: Dict[str, Any]) -> Dict[str, Any]:
    if item.get("kind") not in EVENT_KINDS:
        raise HTTPException(400, f"非法事件种类: {item.get('kind')}")
    mode = item.get("mode")
    if mode is not None and mode not in ("S", "X"):
        raise HTTPException(400, f"非法锁模式: {mode}")
    return item


def _branch_events(branch_id: Optional[int]) -> List[Event]:
    events = database.load_raw_events(conn)
    if branch_id is not None:
        events = database.apply_overrides(events, database.overrides_for(conn, branch_id))
    return events


def _lock_types() -> Dict[str, str]:
    return database.load_lock_types(conn)


def _candidate_choices(
    events: List[Event], candidate: Optional[int]
) -> Optional[Dict[str, str]]:
    if candidate is None:
        return None
    candidates = enumerate_candidates(events, lock_types=_lock_types())
    if candidate < 0 or candidate >= len(candidates):
        raise HTTPException(404, "候选分支不存在")
    return candidates[candidate]["repair_choices"]


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "title": "锁序解剖室"}


@app.get("/api/samples")
def samples() -> Dict[str, Any]:
    return {
        key: {"label": value["label"], "count": len(value["events"])}
        for key, value in SAMPLES.items()
    }


@app.post("/api/samples/{name}/load")
def load_sample(name: str) -> Dict[str, Any]:
    sample = SAMPLES.get(name)
    if sample is None:
        raise HTTPException(404, "样例不存在")
    database.reset_all(conn)
    payload = [_validate_event(dict(item)) for item in sample["events"]]
    ids = database.insert_raw_events(conn, payload)
    return {"loaded": name, "count": len(ids)}


@app.post("/api/events")
def ingest(body: IngestIn) -> Dict[str, Any]:
    if body.reset:
        database.reset_all(conn)
    payload = [_validate_event(item.model_dump()) for item in body.events]
    ids = database.insert_raw_events(conn, payload)
    return {"inserted": len(ids), "ids": ids}


@app.get("/api/events")
def get_events() -> Dict[str, Any]:
    events = database.load_raw_events(conn)
    return {
        "arrival_order": [event.to_dict() for event in events],
        "analysis_order": [event.to_dict() for event in order_events(events)],
    }


@app.post("/api/reset")
def reset() -> Dict[str, str]:
    database.reset_all(conn)
    return {"status": "ok"}


@app.post("/api/lock-types")
def declare_lock_type(body: LockTypeIn) -> Dict[str, Any]:
    if body.kind not in LOCK_KINDS:
        raise HTTPException(400, f"锁类型必须是 {sorted(LOCK_KINDS)}")
    uid = f"{body.gen}:{body.lock}"
    database.upsert_lock_type(conn, uid, body.kind)
    return {"lock_uid": uid, "kind": body.kind}


# ---- 分析结果 -------------------------------------------------------------
@app.get("/api/replay")
def replay(branch_id: Optional[int] = None, candidate: Optional[int] = None) -> Dict[str, Any]:
    events = _branch_events(branch_id)
    choices = _candidate_choices(events, candidate)
    engine = Replay(events, lock_types=_lock_types(), repair_choices=choices)
    engine.run()
    return engine.final_report()


@app.get("/api/candidates")
def candidates(branch_id: Optional[int] = None) -> Dict[str, Any]:
    events = _branch_events(branch_id)
    items = enumerate_candidates(events, lock_types=_lock_types())
    slim: List[Dict[str, Any]] = []
    for index, item in enumerate(items):
        report = item["report"]
        slim.append(
            {
                "index": index,
                "name": item["name"],
                "label": item["label"],
                "repair_choices": item["repair_choices"],
                "anomalies": report["anomalies"],
                "cycle_count": len(report["final"]["cycles"]),
                "final": report["final"],
            }
        )
    return {"candidates": slim}


# ---- 分支（事件校正） -----------------------------------------------------
@app.post("/api/branches")
def create_branch(body: BranchIn) -> Dict[str, Any]:
    branch_id = database.create_branch(conn, body.name, body.parent_id, body.note)
    for override in body.overrides:
        if override.action not in {
            "delete_event",
            "mutate_event",
            "insert_event",
        }:
            raise HTTPException(400, "非法校正动作")
        if override.action == "insert_event":
            _validate_event(override.payload)
        database.add_override(
            conn,
            branch_id,
            override.action,
            override.payload,
            override.event_id,
        )
    return {"branch_id": branch_id, "name": body.name}


@app.get("/api/branches")
def get_branches() -> Dict[str, Any]:
    return {"branches": database.list_branches(conn)}


@app.post("/api/branches/compare")
def compare_branches(body: BranchCompareIn) -> Dict[str, Any]:
    left_events = _branch_events(body.left_id)
    right_events = _branch_events(body.right_id)
    left = {
        "name": f"branch-{body.left_id}",
        "report": _report(left_events),
    }
    right = {
        "name": f"branch-{body.right_id}",
        "report": _report(right_events),
    }
    return diff_branches(left, right)


def _report(events: List[Event]) -> Dict[str, Any]:
    engine = Replay(events, lock_types=_lock_types())
    engine.run()
    return engine.final_report()


# ---- 最小事件片段 ----------------------------------------------------------
@app.post("/api/minimize")
def minimize(body: MinimizeIn) -> Dict[str, Any]:
    events = _branch_events(body.branch_id)
    choices = _candidate_choices(events, body.candidate)
    engine = Replay(events, lock_types=_lock_types(), repair_choices=choices)
    engine.run()
    cycles = engine.final_report()["final"]["cycles"]
    if not cycles:
        raise HTTPException(400, "当前时点没有可复现的环")
    if body.cycle_index >= len(cycles):
        raise HTTPException(400, "环索引越界")
    target = cycles[body.cycle_index]
    reduced = minimize_events(
        events,
        target_cycle=target,
        lock_types=_lock_types(),
        repair_choices=choices,
    )
    verify = Replay(reduced, lock_types=_lock_types(), repair_choices=choices)
    verify.run()
    return {
        "target_cycle": target,
        "original_count": len(order_events(events)),
        "reduced_count": len(reduced),
        "events": [event.to_dict() for event in order_events(reduced)],
        "still_reproduces": any(
            cycle["kind"] == target["kind"]
            and sorted(cycle["threads"]) == sorted(target["threads"])
            and sorted(cycle["locks"]) == sorted(target["locks"])
            for cycle in verify.final_report()["final"]["cycles"]
        ),
    }
