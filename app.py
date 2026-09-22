"""锁序解剖室 —— FastAPI 入口。

运行: uvicorn app:app --host 127.0.0.1 --port 5270
"""
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from locklab import minimize as mz
from locklab.engine import Runner
from locklab.store import Store

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def create_app(db_path=None):
    app = FastAPI(title="锁序解剖室")
    store = Store(db_path or os.environ.get("LOCKLAB_DB",
                                            os.path.join(BASE_DIR, "locklab.db")))
    runners = {}

    def materialize(branch_id, generation):
        events = store.events(generation)
        declared = {}
        for c in (store.corrections_for(branch_id) if branch_id else []):
            op = c.get("op")
            if op == "declare_lock_type" and int(c.get("generation", 0)) == generation:
                declared[c["lock"]] = c["lock_type"]
            elif op == "patch_event":
                for ev in events:
                    if ev["id"] == c.get("event_id"):
                        ev.update(c.get("changes", {}))
            elif op == "drop_event":
                events = [ev for ev in events if ev["id"] != c.get("event_id")]
        return events, declared

    def get_runner(branch_id, generation):
        key = (branch_id, generation)
        if key not in runners:
            events, declared = materialize(branch_id, generation)
            runner = Runner(declared)
            runner.rebuild(events)
            runners[key] = runner
        return runners[key]

    @app.get("/")
    def index():
        return FileResponse(os.path.join(BASE_DIR, "static", "index.html"))

    @app.post("/api/events")
    def post_events(body: dict):
        added = store.add_events(body.get("events", []))
        recompute = {}
        for gen in sorted({e["generation"] for e in added}):
            gen_events = [e for e in added if e["generation"] == gen]
            key = (0, gen)
            if key in runners:
                runners[key].append(gen_events)
            else:
                events, declared = materialize(0, gen)
                runner = Runner(declared)
                runner.rebuild(events)
                runners[key] = runner
            recompute[str(gen)] = dict(runners[key].stats)
            for k in [k for k in runners if k[0] != 0 and k[1] == gen]:
                del runners[k]
        return {"added": len(added), "events": added, "recompute": recompute}

    @app.get("/api/events")
    def list_events(generation: int = 0):
        return {"events": store.events(generation)}

    @app.get("/api/generations")
    def get_generations():
        return {"generations": store.generations()}

    @app.get("/api/graph")
    def graph(branch_id: int = 0, generation: int = 0, at: Optional[int] = None):
        runner = get_runner(branch_id, generation)
        count = None
        if at is not None:
            count = sum(1 for e in runner.events if e["seq"] <= at)
        g, stats = runner.graph_at(count)
        g.update({
            "generation": generation,
            "branch_id": branch_id,
            "event_count": len(runner.events),
            "max_seq": runner.events[-1]["seq"] if runner.events else 0,
            "stats": stats,
        })
        return g

    @app.get("/api/threads")
    def threads(generation: int = 0):
        lanes = {}
        for ev in store.events(generation):
            lanes.setdefault(ev.get("thread") or "?", []).append(ev)
        return {"threads": [{"id": k, "events": v} for k, v in sorted(lanes.items())]}

    @app.post("/api/branches")
    def create_branch(body: dict):
        bid = store.create_branch(body.get("name") or "branch",
                                  int(body.get("parent") or 0),
                                  body.get("corrections") or [])
        return {"branch_id": bid}

    @app.get("/api/branches")
    def list_branches():
        return {"branches": [{"id": 0, "name": "base", "parent": 0}] + store.branches()}

    @app.get("/api/compare")
    def compare(a: int, b: int, generation: int = 0):
        ga, _ = get_runner(a, generation).graph_at()
        gb, _ = get_runner(b, generation).graph_at()

        def cyc_keys(g):
            s = set()
            for c in g["cycles"]:
                s.add("deadlock(%s)[%s]" % (",".join(sorted(c["threads"])),
                                            ",".join(sorted(c["locks"]))))
            for c in g["near_misses"]:
                s.add("near_miss[%s]" % ",".join(sorted(c["locks"])))
            return s

        def wait_keys(g):
            return {(e["from"], e["to"], e["lock"]) for e in g["wait_edges"]}

        def order_keys(g):
            return {(e["from"], e["to"]) for e in g["order_edges"]}

        def anom_keys(g):
            return {(x["kind"], str(x.get("lock")), str(x.get("thread")))
                    for x in g["anomalies"]}

        def diff(f):
            sa, sb = f(ga), f(gb)
            return (sorted(sa - sb, key=str), sorted(sb - sa, key=str))

        ca, cb = diff(cyc_keys)
        wa, wb = diff(wait_keys)
        oa, ob = diff(order_keys)
        aa, ab = diff(anom_keys)
        return {"a": a, "b": b, "generation": generation,
                "cycles_only_a": ca, "cycles_only_b": cb,
                "wait_edges_only_a": wa, "wait_edges_only_b": wb,
                "order_edges_only_a": oa, "order_edges_only_b": ob,
                "anomalies_only_a": aa, "anomalies_only_b": ab}

    @app.post("/api/minimize")
    def api_minimize(body: dict):
        branch_id = int(body.get("branch_id") or 0)
        generation = int(body.get("generation") or 0)
        events, declared = materialize(branch_id, generation)
        _, _, graph = mz.analyze(events, declared)
        target = None
        if body.get("kind") == "near_miss" and body.get("locks"):
            target = ("near_miss", tuple(sorted(body["locks"])))
        elif body.get("threads"):
            for c in graph["cycles"]:
                if sorted(c["threads"]) == sorted(body["threads"]):
                    target = mz.cycle_key(c)
                    break
        if target is None:
            if graph["cycles"]:
                target = mz.cycle_key(graph["cycles"][0])
            elif graph["near_misses"]:
                target = mz.cycle_key(graph["near_misses"][0])
        if target is None:
            raise HTTPException(400, "没有可缩减的环路")
        result = mz.minimize(events, declared, target)
        if result is None:
            raise HTTPException(400, "目标环路不存在")
        return {"target": [list(t) if isinstance(t, tuple) else t for t in target],
                "original_count": len(events), "count": len(result), "events": result}

    return app


app = create_app()
