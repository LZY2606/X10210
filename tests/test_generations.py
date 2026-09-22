def test_same_address_is_different_lock_after_restart(make_timeline):
    tl = make_timeline([
        {"type": "request", "thread": "A", "lock": "0x1", "ts": 1},
        {"type": "acquired", "thread": "A", "lock": "0x1", "ts": 2},
        {"type": "request", "thread": "B", "lock": "0x1", "ts": 3},
        {"type": "restart", "thread": "k", "ts": 4},
        {"type": "request", "thread": "C", "lock": "0x1", "ts": 5},
        {"type": "acquired", "thread": "C", "lock": "0x1", "ts": 6},
    ])
    # 重启前：A 持有、B 等待（同代）
    pre = tl.snapshot(index=3)
    assert {h["gen"] for h in pre["hold_edges"]} == {"g0"}
    assert len(pre["wait_edges"]) == 1
    # 重启后：地址复用，但状态清空，C 的持有独立且无等待
    post = tl.snapshot()
    assert {h["gen"] for h in post["hold_edges"]} == {"g1"}
    assert post["wait_edges"] == []
    assert [(h["thread"], h["lock"]) for h in post["hold_edges"]] == [("C", "0x1")]


def test_explicit_gen_label_change_isolates_state(make_timeline):
    tl = make_timeline([
        {"type": "acquired", "thread": "A", "lock": "L", "gen": "proc1", "ts": 1},
        {"type": "acquired", "thread": "B", "lock": "L", "gen": "proc2", "ts": 2},
    ])
    gens = {e.gen for e in tl.ordered}
    assert gens == {"g0", "g1"}
    post = tl.snapshot()
    assert [(h["thread"], h["gen"]) for h in post["hold_edges"]] == [("B", "g1")]
