from locklab.engine import analyze
from locklab.reduction import reduce_cycle


def deadlock_events():
    return [
        {"id": "t1s", "generation": "g", "timestamp": 1, "seq": 0, "kind": "thread_start", "thread": "T1"},
        {"id": "t2s", "generation": "g", "timestamp": 2, "seq": 1, "kind": "thread_start", "thread": "T2"},
        {"id": "r1", "generation": "g", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "T1", "lock": "L1", "stack": ["w1"]},
        {"id": "g1", "generation": "g", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "T1", "lock": "L1", "stack": ["h1"]},
        {"id": "r2", "generation": "g", "timestamp": 5, "seq": 4, "kind": "acquire_request", "thread": "T2", "lock": "L2", "stack": ["w2"]},
        {"id": "g2", "generation": "g", "timestamp": 6, "seq": 5, "kind": "acquire_success", "thread": "T2", "lock": "L2", "stack": ["h2"]},
        {"id": "r12", "generation": "g", "timestamp": 7, "seq": 6, "kind": "acquire_request", "thread": "T1", "lock": "L2", "stack": ["T1 wait L2"]},
        {"id": "r21", "generation": "g", "timestamp": 8, "seq": 7, "kind": "acquire_request", "thread": "T2", "lock": "L1", "stack": ["T2 wait L1"]},
    ]


def test_recursive_lock_preserves_reentrant_depth_and_wait_stack():
    events = [
        {"id": "r", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "T", "lock": "L"},
        {"id": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "T", "lock": "L", "stack": ["outer"]},
        {"id": "r2", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "T", "lock": "L"},
        {"id": "g2", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "T", "lock": "L", "stack": ["inner"]},
    ]
    result = analyze(events)
    hold = result["snapshots"][-1]["holds"][0]
    assert hold["depth"] == 2
    assert hold["stack"] == ["outer"]
    assert result["snapshots"][-1]["cycles"] == []


def test_rwlock_shared_holders_and_upgrade_blocks_on_other_reader():
    types = {("g", "L"): "rwlock"}
    events = [
        {"id": "r1", "generation": "g", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "T1", "lock": "L", "mode": "shared"},
        {"id": "g1", "generation": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "T1", "lock": "L", "mode": "shared"},
        {"id": "r2", "generation": "g", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "T2", "lock": "L", "mode": "shared"},
        {"id": "g2", "generation": "g", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "T2", "lock": "L", "mode": "shared"},
        {"id": "u", "generation": "g", "timestamp": 5, "seq": 4, "kind": "acquire_request", "thread": "T1", "lock": "L", "mode": "exclusive"},
    ]
    result = analyze(events, types)
    wait = result["snapshots"][-1]["waits"][0]
    assert wait["holder_thread"] == "T2"
    assert wait["request_mode"] == "exclusive"


def test_condition_wait_temporarily_releases_and_reacquires():
    events = [
        {"id": "r", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "T", "lock": "L"},
        {"id": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "T", "lock": "L"},
        {"id": "w", "timestamp": 3, "seq": 2, "kind": "cond_wait", "thread": "T", "lock": "L", "cond": "C", "stack": ["wait"]},
        {"id": "n", "timestamp": 4, "seq": 3, "kind": "cond_notify", "thread": "N", "cond": "C"},
        {"id": "rg", "timestamp": 5, "seq": 4, "kind": "acquire_success", "thread": "T", "lock": "L", "request_id": "w:reacquire"},
    ]
    waiting = analyze(events[:3])
    assert waiting["snapshots"][-1]["holds"] == []
    assert waiting["snapshots"][-1]["waits"][0]["kind"] == "condition_wait"
    notified = analyze(events[:4])
    assert notified["snapshots"][-1]["waits"][0]["kind"] == "waits_for"
    resumed = analyze(events)
    assert resumed["snapshots"][-1]["holds"][0]["thread"] == "T"


def test_timeout_cancels_wait_without_release_fabrication():
    events = [
        {"id": "r", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "T", "lock": "L"},
        {"id": "to", "timestamp": 2, "seq": 1, "kind": "acquire_timeout", "thread": "T", "lock": "L", "request_id": "r"},
    ]
    result = analyze(events)
    assert result["snapshots"][-1]["waits"] == []
    assert result["snapshots"][-1]["holds"] == []


def test_thread_end_cancels_pending_wait_and_flags_held_lock():
    events = [
        {"id": "r", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "T", "lock": "L"},
        {"id": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "T", "lock": "L"},
        {"id": "e", "timestamp": 3, "seq": 2, "kind": "thread_end", "thread": "T"},
    ]
    result = analyze(events)
    snapshot = result["snapshots"][-1]
    assert snapshot["waits"] == []
    assert snapshot["holds"][0]["thread"] == "T"
    assert any(item["code"] == "held_lock_at_thread_end" for item in snapshot["anomalies"])


def test_same_address_in_different_generation_is_distinct():
    events = deadlock_events() + [
        {"id": "new", "generation": "g2", "timestamp": 1, "seq": 8, "kind": "acquire_request", "thread": "T3", "lock": "L1"},
    ]
    result = analyze(events)
    assert len(result["generations"]) == 2
    final = result["snapshots"][-1]
    assert final["holds"] == []
    assert final["waits"] == []


def test_missing_release_remains_candidate_and_does_not_invent_event():
    events = deadlock_events()[:6]
    result = analyze(events)
    assert len([item for item in result["events"] if item["kind"] == "release"]) == 0
    assert result["snapshots"][-1]["holds"]


def test_out_of_order_is_marked_while_preserving_arrival_order():
    events = [
        {"id": "late", "timestamp": 1, "seq": 1, "kind": "thread_start", "thread": "T"},
        {"id": "first", "timestamp": 2, "seq": 0, "kind": "thread_start", "thread": "T2"},
    ]
    result = analyze(events)
    assert result["raw_order"][0]["id"] == "first"
    assert result["events"][0]["id"] == "late"
    assert result["out_of_order"] == ["late"]


def test_multiple_cycles_coexist_and_include_threads_stacks():
    events = deadlock_events() + [
        {"id": "r3", "timestamp": 9, "seq": 8, "kind": "acquire_request", "thread": "T3", "lock": "L3", "stack": ["h3"]},
        {"id": "g3", "timestamp": 10, "seq": 9, "kind": "acquire_success", "thread": "T3", "lock": "L3"},
        {"id": "r4", "timestamp": 11, "seq": 10, "kind": "acquire_request", "thread": "T4", "lock": "L4", "stack": ["h4"]},
        {"id": "g4", "timestamp": 12, "seq": 11, "kind": "acquire_success", "thread": "T4", "lock": "L4"},
        {"id": "r34", "timestamp": 13, "seq": 12, "kind": "acquire_request", "thread": "T3", "lock": "L4"},
        {"id": "r43", "timestamp": 14, "seq": 13, "kind": "acquire_request", "thread": "T4", "lock": "L3"},
    ]
    result = analyze(events)
    cycles = result["snapshots"][-1]["cycles"]
    assert len(cycles) == 2
    assert {tuple(cycle["threads"]) for cycle in cycles} == {("T1", "T2"), ("T3", "T4")}
    assert all(edge["stack"] and edge["hold_stack"] for cycle in cycles for edge in cycle["edges"])


def test_reduction_keeps_pairing_and_condition_semantics():
    events = [
        {"id": "noise", "timestamp": 0, "seq": 0, "kind": "thread_start", "thread": "NOISE"},
        *deadlock_events(),
    ]
    reduced = reduce_cycle(events)
    assert reduced["event_count"] == 8
    kinds = {event["kind"] for event in reduced["events"]}
    assert "release" not in kinds
    assert reduced["cycle"]["threads"] == ["T1", "T2"]
