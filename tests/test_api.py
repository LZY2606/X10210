def _abba():
    return [
        {"id": "s1", "type": "thread_start", "thread": "A", "ts": 1},
        {"id": "s2", "type": "thread_start", "thread": "B", "ts": 1},
        {"id": "r1", "type": "request", "thread": "A", "lock": "La", "ts": 2},
        {"id": "a1", "type": "acquired", "thread": "A", "lock": "La", "ts": 3},
        {"id": "r2", "type": "request", "thread": "B", "lock": "Lb", "ts": 4},
        {"id": "a2", "type": "acquired", "thread": "B", "lock": "Lb", "ts": 5},
        {"id": "r3", "type": "request", "thread": "A", "lock": "Lb", "ts": 6},
        {"id": "r4", "type": "request", "thread": "B", "lock": "La", "ts": 7},
    ]


def test_index_title(client, reset_db):
    reset_db()
    page = client.get("/").text
    assert "锁序解剖室" in page
    assert "<title>锁序解剖室</title>" in page


def test_import_state_cycles_and_minimize(client, reset_db):
    reset_db()
    r = client.post("/api/import", json={"events": _abba(), "reset": True})
    assert r.json()["imported"] == 8
    cycles = client.get("/api/cycles").json()["moments"]
    assert len(cycles) == 1 and cycles[0]["signature"] == "A->B->A"
    state = client.get("/api/state").json()
    assert state["confidence"] == "high"
    minim = client.get("/api/minimize", params={"cycle": "A->B->A"}).json()
    assert minim["reproduced"] and minim["reduced_count"] < minim["original_count"]


def test_streaming_append_recomputes(client, reset_db):
    reset_db()
    client.post("/api/import", json={"events": _abba()[:6], "reset": True})
    assert client.get("/api/cycles").json()["moments"] == []
    r = client.post("/api/append", json={"events": _abba()[6:]})
    assert r.json()["added"] == 2
    sigs = [m["signature"] for m in client.get("/api/cycles").json()["moments"]]
    assert sigs == ["A->B->A"]


def test_correction_branch_and_compare(client, reset_db):
    reset_db()
    client.post("/api/import", json={"events": _abba(), "reset": True})
    # 删除 B 请求 La 的事件 r4，环消失
    r = client.post("/api/branches", json={
        "name": "fix-r4",
        "parent": "main",
        "correction": {"remove": ["r4"]},
    })
    assert r.status_code == 200
    assert client.get("/api/cycles", params={"branch": "fix-r4"}).json()["moments"] == []
    cmp_ = client.get("/api/compare", params={"a": "main", "b": "fix-r4"}).json()
    assert cmp_["cycles_equal"] is False
    assert "A->B->A" in cmp_["a"]["cycle_signatures"]
    assert cmp_["b"]["cycle_signatures"] == []
    # 并列重建都保留
    names = [b["name"] for b in client.get("/api/branches").json()["branches"]]
    assert "main" in names and "fix-r4" in names


def test_declare_rwlock_changes_graph(client, reset_db):
    reset_db()
    events = [
        {"type": "request", "thread": "R1", "lock": "RW", "mode": "read", "ts": 1},
        {"type": "acquired", "thread": "R1", "lock": "RW", "mode": "read", "ts": 2},
        {"type": "request", "thread": "R2", "lock": "RW", "mode": "read", "ts": 3},
        {"type": "acquired", "thread": "R2", "lock": "RW", "mode": "read", "ts": 4},
    ]
    client.post("/api/import", json={"events": events, "reset": True})
    before = client.get("/api/state").json()
    # 未知类型时读者按互斥独占处理，R2 会被阻塞
    assert len(before["wait_edges"]) == 1
    client.post("/api/declare", json={"lock": "RW", "kind": "rwlock"})
    after = client.get("/api/state").json()
    assert after["wait_edges"] == []
    assert len(after["hold_edges"]) == 2


def test_sample_endpoint(client, reset_db):
    reset_db()
    r = client.post("/api/sample")
    assert r.json()["loaded"] > 0
    sigs = {m["signature"] for m in client.get("/api/cycles").json()["moments"]}
    assert "A->B->A" in sigs
