"""API 与页面测试。"""
import os
import tempfile

os.environ["LOCKLAB_DB"] = tempfile.mktemp(prefix="locklab_test_", suffix=".db")

from fastapi.testclient import TestClient  # noqa: E402

import app as app_module  # noqa: E402

client = TestClient(app_module.app)


def test_index_page_has_title():
    r = client.get("/")
    assert r.status_code == 200
    assert "锁序解剖室" in r.text


def _main_branch():
    return client.get("/api/branches").json()["main"]


def test_demo_graph_and_cycle_evidence():
    main = _main_branch()
    r = client.post("/api/demo")
    assert r.status_code == 200
    g = client.get(f"/api/graph?branch={main}&t=1000").json()
    sets = {frozenset(c["threads"]) for c in g["cycles"]}
    assert frozenset({"T1", "T2"}) in sets
    # 跨代次地址复用不产生假环
    assert all not in [c["threads"] for c in g["cycles"]]
    assert not any("U1" in c["threads"] or "U2" in c["threads"] for c in g["cycles"])
    cyc = next(c for c in g["cycles"] if set(c["threads"]) == {"T1", "T2"})
    # 证据：具体线程、锁、持有栈、等待栈
    assert set(cyc["locks"]) == {"1:A", "1:B"}
    assert cyc["detail"]["T1"]["waiting_for"]["lock"] == "1:B"
    assert any(h["lock"] == "1:A" for h in cyc["detail"]["T1"]["holding"])
    assert cyc["detail"]["T2"]["waiting_for"]["since_event"] is not None
    # 每条等待边都能回到产生它的事件
    for e in g["wait_for"]:
        assert e["wait_event"] is not None


def test_timeline_and_time_slider():
    main = _main_branch()
    tl = client.get(f"/api/timeline?branch={main}").json()
    assert "T1" in tl["threads"] and tl["t_max"] > tl["t_min"]
    early = client.get(f"/api/graph?branch={main}&t=2").json()
    assert early["cycles"] == []  # 死锁尚未形成
    late = client.get(f"/api/graph?branch={main}&t=6").json()
    assert any(set(c["threads"]) == {"T1", "T2"} for c in late["cycles"])


def test_branch_correction_and_compare():
    main = _main_branch()
    tl = client.get(f"/api/timeline?branch={main}").json()
    target = next(e for e in tl["events"] if e.get("wait_id") == "w4")  # T2 请求 A
    r = client.post("/api/branches", json={
        "name": "fix-t2", "parent": main,
        "corrections": [{"event_id": target["id"], "patch": {"lock": "B"}}],
        "note": "校正：T2 改为请求已持有的 B（可重入）"})
    assert r.status_code == 200
    branch = r.json()["branch"]
    g = client.get(f"/api/graph?branch={branch}&t=1000").json()
    assert not any(set(c["threads"]) == {"T1", "T2"} for c in g["cycles"])  # 校正解开了环
    cmp = client.get(f"/api/compare?a={main}&b={branch}&t=1000").json()
    assert cmp["event_diff"]  # 事件差异
    assert cmp["wait_only_a"]  # 原分支独有的等待边
    # 两个并列的合法重建都保留
    names = {b["name"] for b in client.get("/api/branches").json()["branches"]}
    assert {"main", "fix-t2"} <= names


def test_lock_type_declaration_branch():
    main = _main_branch()
    r = client.post("/api/branches", json={
        "name": "rw", "parent": main,
        "declarations": [{"epoch": 1, "lock": "A", "ltype": "rwlock"}]})
    assert r.status_code == 200
    bad = client.post("/api/branches", json={
        "name": "bad", "parent": main,
        "declarations": [{"epoch": 1, "lock": "A", "ltype": "spinlock"}]})
    assert bad.status_code == 400


def test_minimize_endpoint():
    main = _main_branch()
    r = client.post("/api/minimize", json={"branch": main})
    assert r.status_code == 200
    d = r.json()
    assert set(d["threads"]) == {"T1", "T2"}
    assert d["reduced"] < d["original"]
    types = [e["type"] for e in d["events"]]
    assert "acquire" in types and "acquire_request" in types


def test_stream_endpoint_incremental():
    main = _main_branch()
    batch = []
    for k in range(6):
        batch += [
            {"type": "acquire_request", "ts": 300 + k * 3, "thread": f"S{k % 2}",
             "lock": f"K{k}", "wait_id": f"s{k}"},
            {"type": "acquire", "ts": 301 + k * 3, "thread": f"S{k % 2}",
             "lock": f"K{k}", "wait_id": f"s{k}"},
            {"type": "release", "ts": 302 + k * 3, "thread": f"S{k % 2}", "lock": f"K{k}"},
        ]
    r1 = client.post("/api/stream?epoch=3", json={"branch": main, "events": batch})
    assert r1.status_code == 200
    stats1 = r1.json()["stats"]
    r2 = client.post("/api/stream?epoch=3", json={"branch": main, "events": batch[:3]})
    stats2 = r2.json()["stats"]
    # 第二次只重算受影响尾段（追加的 3 个 + 尾段余量），而非全部事件
    delta = stats2["events_replayed"] - stats1["events_replayed"]
    assert 3 <= delta <= 3 + 15
    assert delta < stats2["events_replayed"]
