"""递归锁、读写升级、condition、超时取消、线程退出、缺 release、乱序、多环。"""


def _cycle_sigs(tl, index=None):
    return {c["signature"] for c in tl.snapshot(index)["cycles"]}


def test_reentrant_mutex(make_timeline):
    tl = make_timeline([
        {"type": "thread_start", "thread": "T", "ts": 1},
        {"type": "request", "thread": "T", "lock": "L", "ts": 2},
        {"type": "acquired", "thread": "T", "lock": "L", "ts": 3},
        {"type": "request", "thread": "T", "lock": "L", "ts": 4},
        {"type": "acquired", "thread": "T", "lock": "L", "ts": 5},
        {"type": "release", "thread": "T", "lock": "L", "ts": 6},
    ])
    snap = tl.snapshot()
    # 只释放一次 -> 仍然持有（重入计数为 1），且无等待/无环
    assert snap["confidence"] == "high"
    assert [(h["thread"], h["lock"], h["count"]) for h in snap["hold_edges"]] == [
        ("T", "L", 1)
    ]
    assert snap["wait_edges"] == []


def test_rwlock_shared_then_exclusive_conflict(make_timeline):
    tl = make_timeline(
        [
            {"type": "request", "thread": "R1", "lock": "RW", "mode": "read", "ts": 1},
            {"type": "acquired", "thread": "R1", "lock": "RW", "mode": "read", "ts": 2},
            {"type": "request", "thread": "R2", "lock": "RW", "mode": "read", "ts": 3},
            {"type": "acquired", "thread": "R2", "lock": "RW", "mode": "read", "ts": 4},
            {"type": "request", "thread": "W", "lock": "RW", "mode": "write", "ts": 5},
        ],
        {"RW": "rwlock"},
    )
    snap = tl.snapshot()
    assert {e["thread"] for e in snap["hold_edges"]} == {"R1", "R2"}
    waits = {(w["thread"], w["blocker"]) for w in snap["wait_edges"]}
    assert ("W", "R1") in waits and ("W", "R2") in waits


def test_rwlock_upgrade_deadlock(make_timeline):
    tl = make_timeline(
        [
            {"type": "request", "thread": "R1", "lock": "RW", "mode": "read", "ts": 1,
             "stack": ["r1-read"]},
            {"type": "acquired", "thread": "R1", "lock": "RW", "mode": "read", "ts": 2},
            {"type": "request", "thread": "R2", "lock": "RW", "mode": "read", "ts": 3,
             "stack": ["r2-read"]},
            {"type": "acquired", "thread": "R2", "lock": "RW", "mode": "read", "ts": 4},
            {"type": "request", "thread": "R1", "lock": "RW", "mode": "write", "ts": 5,
             "stack": ["r1-upgrade"]},
            {"type": "request", "thread": "R2", "lock": "RW", "mode": "write", "ts": 6,
             "stack": ["r2-upgrade"]},
        ],
        {"RW": "rwlock"},
    )
    sigs = _cycle_sigs(tl)
    assert sigs == {"R1->R2->R1"}
    cycle = tl.snapshot()["cycles"][0]
    for step in cycle["steps"]:
        assert step["upgrading"] is True
        assert step["holding_lock"] == "RW"
        assert step["wait_stack"], "等待栈必须随环证据给出"
        assert step["hold_stack"], "持有栈必须随环证据给出"


def test_condition_wait_releases_then_reacquires(make_timeline):
    tl = make_timeline([
        {"type": "request", "thread": "C", "lock": "M", "cond": "cv", "ts": 1},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 2},
        {"type": "cond_wait", "thread": "C", "lock": "M", "cond": "cv", "ts": 3},
    ])
    waiting = tl.snapshot()
    assert waiting["hold_edges"] == []
    assert [p["thread"] for p in waiting["parked_edges"]] == ["C"]

    # notify 后线程重新竞争；在别人释放后 acquired 成功
    tl2 = make_timeline([
        {"type": "request", "thread": "C", "lock": "M", "cond": "cv", "ts": 1},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 2},
        {"type": "cond_wait", "thread": "C", "lock": "M", "cond": "cv", "ts": 3},
        {"type": "request", "thread": "S", "lock": "M", "ts": 4},
        {"type": "acquired", "thread": "S", "lock": "M", "ts": 5},
        {"type": "cond_notify", "thread": "S", "lock": "M", "cond": "cv", "ts": 6},
        {"type": "release", "thread": "S", "lock": "M", "ts": 7},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 8},
        {"type": "release", "thread": "C", "lock": "M", "ts": 9},
    ])
    snap = tl2.snapshot()
    assert snap["hold_edges"] == []
    assert snap["wait_edges"] == []
    assert snap["confidence"] == "high"


def test_timeout_revokes_wait(make_timeline):
    tl = make_timeline([
        {"type": "request", "thread": "O", "lock": "L", "ts": 1},
        {"type": "acquired", "thread": "O", "lock": "L", "ts": 2},
        {"type": "request", "thread": "W", "lock": "L", "ts": 3},
        {"type": "timeout", "thread": "W", "lock": "L", "ts": 4},
    ])
    snap = tl.snapshot()
    assert snap["wait_edges"] == []
    assert [(h["thread"], h["lock"]) for h in snap["hold_edges"]] == [("O", "L")]


def test_thread_exit_revokes_wait_but_keeps_leaked_hold(make_timeline):
    tl = make_timeline([
        {"type": "thread_start", "thread": "O", "ts": 1},
        {"type": "thread_start", "thread": "W", "ts": 1},
        {"type": "request", "thread": "O", "lock": "L", "ts": 2},
        {"type": "acquired", "thread": "O", "lock": "L", "ts": 3},
        {"type": "request", "thread": "W", "lock": "L", "ts": 4},
        {"type": "thread_end", "thread": "W", "ts": 5},
        {"type": "thread_end", "thread": "O", "ts": 6},
    ])
    snap = tl.snapshot()
    assert snap["wait_edges"] == []  # 等待随线程退出撤销
    # 绝不凭空补 release：退出时仍持有的锁保留为候选持有
    assert [(h["thread"], h["lock"]) for h in snap["hold_edges"]] == [("O", "L")]
    codes = {a["code"] for a in snap["anomalies"]}
    assert "wait_revoked_exit" in codes
    assert "thread_exit_holding" in codes


def test_missing_release_never_fabricated(make_timeline):
    tl = make_timeline([
        {"type": "request", "thread": "A", "lock": "L", "ts": 1},
        {"type": "acquired", "thread": "A", "lock": "L", "ts": 2},
        {"type": "release", "thread": "A", "lock": "L", "ts": 3},
        {"type": "release", "thread": "A", "lock": "L", "ts": 4},
    ])
    snap = tl.snapshot()
    codes = [a["code"] for a in snap["anomalies"]]
    assert "release_without_hold" in codes


def test_out_of_order_marked_tentative(make_timeline):
    # 到达顺序 1..4，其中 ts=2 的事件晚于 ts=9 到达 -> late
    tl = make_timeline([
        {"type": "request", "thread": "A", "lock": "L1", "ts": 1},
        {"type": "acquired", "thread": "A", "lock": "L1", "ts": 9},
        {"type": "request", "thread": "B", "lock": "L2", "ts": 2},
        {"type": "acquired", "thread": "B", "lock": "L2", "ts": 3},
    ])
    assert [e.late for e in tl.ordered].count(True) >= 1
    assert tl.snapshot()["confidence"] == "tentative"


def test_two_cycles_coexist(make_timeline):
    events = []
    ts = 0

    def add(t, th, l=None, **kw):
        nonlocal ts
        ts += 1
        events.append({"type": t, "thread": th, "lock": l, "ts": ts, **kw})

    add("request", "A", "La")
    add("acquired", "A", "La")
    add("request", "B", "Lb")
    add("acquired", "B", "Lb")
    add("request", "A", "Lb")
    add("request", "B", "La")
    add("request", "C", "Lc")
    add("acquired", "C", "Lc")
    add("request", "D", "Ld")
    add("acquired", "D", "Ld")
    add("request", "C", "Ld")
    add("request", "D", "Lc")
    tl = make_timeline(events)
    sigs = _cycle_sigs(tl)
    assert sigs == {"A->B->A", "C->D->C"}
