"""API 与页面验收测试。"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_tmp = tempfile.mkdtemp()
os.environ["LOCKLAB_DB"] = os.path.join(_tmp, "test.db")

from fastapi.testclient import TestClient
import app as app_module

client = TestClient(app_module.app)


def deadlock_events():
    return [
        {"ts": 1, "thread": "T1", "type": "thread_start"},
        {"ts": 1, "thread": "T2", "type": "thread_start"},
        {"ts": 2, "thread": "T1", "lock": "0xA", "type": "acquire_request"},
        {"ts": 3, "thread": "T1", "lock": "0xA", "type": "acquire_success"},
        {"ts": 4, "thread": "T2", "lock": "0xB", "type": "acquire_request"},
        {"ts": 5, "thread": "T2", "lock": "0xB", "type": "acquire_success"},
        {"ts": 6, "thread": "T1", "lock": "0xB", "type": "acquire_request"},
        {"ts": 7, "thread": "T2", "lock": "0xA", "type": "acquire_request"},
    ]


def test_index_page_shows_title():
    r = client.get("/")
    assert r.status_code == 200
    assert "锁序解剖室" in r.text


def test_import_graph_cycles_flow():
    r = client.post("/api/import", json={"generation": 1, "events": deadlock_events()})
    assert r.json()["ok"]
    g = client.get("/api/graph", params={"branch": 0, "t": 100}).json()
    assert len(g["edges"]) == 2
    # 每条边都能回到产生它的事件
    for e in g["edges"]:
        assert e["wait_event"] and e["hold_event"]
    c = client.get("/api/cycles", params={"branch": 0, "t": 100}).json()
    assert len(c["cycles"]) == 1
    cyc = c["cycles"][0]
    assert sorted(cyc["threads"]) == ["T1", "T2"]
    for edge in cyc["edges"]:
        assert edge["held_stack"] and edge["wait_stack"]
    # 时点选择：死锁形成前无环
    early = client.get("/api/cycles", params={"branch": 0, "t": 5}).json()
    assert early["cycles"] == []


def test_correction_creates_parallel_branch():
    base = client.get("/api/cycles", params={"branch": 0, "t": 100}).json()
    assert len(base["cycles"]) == 1
    # 校正：把 T2 等待 0xA 的请求改成等待 0xC -> 新分支环消失，基分支保留
    tl = client.get("/api/timeline", params={"branch": 0}).json()
    target = [e for e in tl["events"] if e["thread"] == "T2" and e["type"] == "acquire_request" and e["lock"] == "0xA"][0]
    r = client.post("/api/corrections", json={
        "branch": 0, "kind": "edit_event", "event_id": target["id"],
        "fields": {"lock": "0xC"}, "note": "修正锁地址"})
    new_branch = r.json()["branch"]
    assert new_branch != 0
    fixed = client.get("/api/cycles", params={"branch": new_branch, "t": 100}).json()
    assert fixed["cycles"] == []
    still = client.get("/api/cycles", params={"branch": 0, "t": 100}).json()
    assert len(still["cycles"]) == 1
    # 分支比较
    cmp_ = client.get("/api/compare", params={"a": 0, "b": new_branch, "t": 100}).json()
    assert len(cmp_["only_a"]) == 1
    assert cmp_["only_b"] == []
    branches = client.get("/api/branches").json()["branches"]
    assert len(branches) >= 2


def test_declare_lock_type_correction():
    r = client.post("/api/corrections", json={
        "branch": 0, "kind": "declare_lock",
        "generation": 1, "address": "0xA", "ltype": "recursive",
        "note": "声明递归锁"})
    assert r.json()["ok"]
    lt = app_module.store.lock_types(r.json()["branch"])
    assert lt["1:0xA"] == "recursive"
    assert app_module.store.lock_types(0) == {}


def test_minimize_endpoint():
    noise = [{"ts": 20 + i, "thread": "T9", "lock": "0xN%d" % i,
              "type": t} for i in range(5) for t in
             ("acquire_request", "acquire_success", "release")]
    client.post("/api/events/append", json={"generation": 1, "events": noise})
    before = client.get("/api/timeline", params={"branch": 0}).json()
    r = client.post("/api/minimize", json={"branch": 0, "t": 100, "cycle_index": 0})
    d = r.json()
    assert d["ok"]
    assert d["count"] < d["original_count"] == len(before["events"])
    # 缩减结果仍复现同一环
    from locklab.engine import Engine
    engine = Engine(d["events"])
    cycles = engine.cycles_at(100)
    assert len(cycles) == 1
    assert tuple(sorted(cycles[0]["signature"])) == tuple(sorted(tuple(x) for x in d["signature"]))
