"""锁序解剖室 —— FastAPI 入口。"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from lab import sample
from lab.minimize import minimize
from lab.storage import (
    append_events,
    connect,
    declarations,
    get_branch,
    init_db,
    list_branches,
    load_events,
    materialize_branch,
    replace_all_events,
    require_branch,
)
from lab.timeline import Timeline

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")

app = FastAPI(title="锁序解剖室", version="1.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")


def get_conn() -> Any:
    conn = connect(DB_PATH)
    init_db(conn)
    return conn


def build_timeline(conn: Any, branch_name: str) -> Timeline:
    branch = require_branch(conn, branch_name)
    events = load_events(conn, branch["id"])
    types = declarations(conn, branch["id"])
    return Timeline(events, types)


class ImportRequest(BaseModel):
    events: List[Dict[str, Any]] = Field(default_factory=list)
    branch: str = "main"
    reset: bool = False


class AppendRequest(BaseModel):
    events: List[Dict[str, Any]]
    branch: str = "main"


class BranchRequest(BaseModel):
    name: str
    parent: str = "main"
    correction: Dict[str, Any] = Field(default_factory=dict)
    note: str = ""


class DeclareRequest(BaseModel):
    lock: str
    kind: str  # mutex / rwlock
    branch: str = "main"


@app.get("/")
def index() -> Any:
    return FileResponse("static/index.html")


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "锁序解剖室"}


@app.get("/api/branches")
def api_branches() -> Any:
    conn = get_conn()
    try:
        return {"branches": list_branches(conn)}
    finally:
        conn.close()


@app.post("/api/import")
def api_import(req: ImportRequest) -> Any:
    conn = get_conn()
    try:
        if get_branch(conn, req.branch) is None and req.branch != "main":
            raise HTTPException(404, "branch not found: %s" % req.branch)
        branch = require_branch(conn, req.branch)
        if req.reset:
            events = replace_all_events(conn, branch["id"], req.events)
        else:
            events = append_events(conn, branch["id"], req.events)
        return {"imported": len(events), "total": len(load_events(conn, branch["id"]))}
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    finally:
        conn.close()


@app.post("/api/append")
def api_append(req: AppendRequest) -> Any:
    conn = get_conn()
    try:
        branch = require_branch(conn, req.branch)
        events = append_events(conn, branch["id"], req.events)
        timeline = build_timeline(conn, req.branch)
        return {
            "added": len(events),
            "total": timeline.snapshot()["total"],
            "affected_from": len(timeline.ordered),
            "late_seqs": [e.seq for e in timeline.ordered if e.late],
        }
    finally:
        conn.close()


@app.post("/api/branches")
def api_create_branch(req: BranchRequest) -> Any:
    conn = get_conn()
    try:
        info = materialize_branch(
            conn, req.parent, req.name, req.correction, req.note
        )
        return info
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    finally:
        conn.close()


@app.get("/api/state")
def api_state(branch: str = "main", index: Optional[int] = None) -> Any:
    conn = get_conn()
    try:
        timeline = build_timeline(conn, branch)
        return timeline.snapshot(index)
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    finally:
        conn.close()


@app.get("/api/cycles")
def api_cycles(branch: str = "main") -> Any:
    conn = get_conn()
    try:
        timeline = build_timeline(conn, branch)
        return {"moments": timeline.cycle_moments()}
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    finally:
        conn.close()


@app.get("/api/timeline")
def api_timeline(branch: str = "main") -> Any:
    conn = get_conn()
    try:
        timeline = build_timeline(conn, branch)
        data = timeline.swimlanes()
        data["events"] = [e.to_dict() for e in timeline.ordered]
        return data
    finally:
        conn.close()


@app.get("/api/compare")
def api_compare(a: str, b: str) -> Any:
    conn = get_conn()
    try:
        ta = build_timeline(conn, a)
        tb = build_timeline(conn, b)
        sa, sb = ta.final().snapshot(), tb.final().snapshot()
        return {
            "a": {"branch": a, **_summary(sa)},
            "b": {"branch": b, **_summary(sb)},
            "cycles_equal": {c["signature"] for c in sa["cycles"]}
            == {c["signature"] for c in sb["cycles"]},
            "events_a": [e.to_dict() for e in ta.ordered],
            "events_b": [e.to_dict() for e in tb.ordered],
        }
    finally:
        conn.close()


def _summary(snap: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "confidence": snap["confidence"],
        "cycle_signatures": [c["signature"] for c in snap["cycles"]],
        "hold_count": len(snap["hold_edges"]),
        "wait_count": len(snap["wait_edges"]),
        "anomalies": snap["anomalies"],
    }


@app.get("/api/minimize")
def api_minimize(branch: str = "main", cycle: Optional[str] = None) -> Any:
    conn = get_conn()
    try:
        timeline = build_timeline(conn, branch)
        return minimize(timeline, cycle)
    finally:
        conn.close()


@app.post("/api/declare")
def api_declare(req: DeclareRequest) -> Any:
    if req.kind not in ("mutex", "rwlock"):
        raise HTTPException(400, "kind must be mutex or rwlock")
    conn = get_conn()
    try:
        branch = require_branch(conn, req.branch)
        conn.execute(
            """INSERT INTO declarations (branch_id, lock_addr, kind)
               VALUES (?,?,?)
               ON CONFLICT(branch_id, lock_addr)
               DO UPDATE SET kind=excluded.kind""",
            (branch["id"], req.lock, req.kind),
        )
        conn.commit()
        return {"lock": req.lock, "kind": req.kind}
    finally:
        conn.close()


@app.post("/api/sample")
def api_sample() -> Any:
    """载入内置演示轨迹（递归锁 + ABBA + condition + 超时 + 重启复用）。"""
    conn = get_conn()
    try:
        branch = require_branch(conn, "main")
        replace_all_events(conn, branch["id"], sample.demo_events())
        for lock, kind in sample.demo_declarations().items():
            conn.execute(
                """INSERT INTO declarations (branch_id, lock_addr, kind)
                   VALUES (?,?,?)
                   ON CONFLICT(branch_id, lock_addr)
                   DO UPDATE SET kind=excluded.kind""",
                (branch["id"], lock, kind),
            )
        conn.commit()
        timeline = build_timeline(conn, "main")
        return {"loaded": timeline.snapshot()["total"]}
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5270)
