from app.models import RECURSIVE, RWLOCK, FIRM, LOW, TENTATIVE, EXCLUSIVE, SHARED
from app.engine import (
    A_THREAD_LEAK, A_SELF_DEADLOCK, A_ORPHAN_GRANT, A_ORPHAN_RELEASE,
    A_UPGRADE_WHILE_READER, A_CONFLICT_GRANT, A_COND_WAIT_NO_LOCK,
    A_COND_GRANT_NO_WAIT, A_CANCEL_NOTHING,
)
from tests.helpers import (
    ev, reset_ids, starts, run, cycle_sigs,
    THREAD_START, THREAD_END, REQUEST, GRANT, RELEASE,
    COND_WAIT, COND_NOTIFY, COND_GRANT, CANCEL,
)


def setup_function(_):
    reset_ids()


def abba(t1="T1", t2="T2", a="A", b="B", gen=1, t=1000):
    """Classic two-thread ABBA deadlock ending while blocked."""
    return starts([t1, t2], gen=gen, t0=t) + [
        ev(REQUEST, t + 10, t1, a, gen=gen, stack=["a:1"]),
        ev(GRANT, t + 11, t1, a, gen=gen),
        ev(REQUEST, t + 12, t2, b, gen=gen, stack=["b:1"]),
        ev(GRANT, t + 13, t2, b, gen=gen),
        ev(REQUEST, t + 20, t1, b, gen=gen, stack=["a:2"]),
        ev(REQUEST, t + 21, t2, a, gen=gen, stack=["b:2"]),
    ]


def test_recursive_lock_reentrant_no_cycle():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "R"),
        ev(GRANT, 11, "T", "R"),
        ev(REQUEST, 12, "T", "R"),
        ev(REQUEST, 13, "T", "R"),
        ev(RELEASE, 20, "T", "R"),
        ev(RELEASE, 21, "T", "R"),
        ev(RELEASE, 22, "T", "R"),
    ]
    res = run(events, {"1:R": RECURSIVE})
    assert not res.anomalies
    assert not cycle_sigs(res)
    gs = res.gens[1]
    assert "R" not in gs.holders["T"]


def test_recursive_lock_unbalanced_release_still_present():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "R"),
        ev(GRANT, 11, "T", "R"),
        ev(REQUEST, 12, "T", "R"),
        ev(RELEASE, 20, "T", "R"),
        ev(RELEASE, 21, "T", "R"),
    ]
    res = run(events, {"1:R": RECURSIVE})
    # depth 2 minus 2 releases balances exactly
    assert not res.anomalies
    assert "R" not in res.gens[1].holders["T"]
    # one extra release is an orphan (we never invent a matching acquire)
    events.append(ev(RELEASE, 22, "T", "R"))
    res2 = run(events, {"1:R": RECURSIVE})
    assert any(a.kind == A_ORPHAN_RELEASE for a in res2.anomalies)


def test_mutex_self_acquisition_is_self_deadlock():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "M"),
        ev(GRANT, 11, "T", "M"),
        ev(REQUEST, 12, "T", "M"),
    ]
    res = run(events, {"1:M": "mutex"})
    assert any(a.kind == A_SELF_DEADLOCK and a.confidence == LOW
               for a in res.anomalies)


def test_rwlock_shared_readers_coexist():
    events = starts(["R1", "R2"]) + [
        ev(REQUEST, 10, "R1", "L", mode=SHARED),
        ev(GRANT, 11, "R1", "L", mode=SHARED),
        ev(REQUEST, 12, "R2", "L", mode=SHARED),
        ev(GRANT, 13, "R2", "L", mode=SHARED),
    ]
    res = run(events, {"1:L": RWLOCK})
    assert not res.anomalies
    assert len(res.gens[1].holders) == 2


def test_rwlock_writer_blocks_readers_and_vice_versa():
    events = starts(["W", "R"]) + [
        ev(REQUEST, 10, "W", "L", mode=EXCLUSIVE),
        ev(GRANT, 11, "W", "L", mode=EXCLUSIVE),
        ev(REQUEST, 12, "R", "L", mode=SHARED),  # waits
    ]
    res = run(events, {"1:L": RWLOCK})
    assert "R" in res.gens[1].waiting["L"]
    assert "L" not in res.gens[1].holders.get("R", {})


def test_rwlock_upgrade_sole_reader_succeeds():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "L", mode=SHARED, stack=["read"]),
        ev(GRANT, 11, "T", "L", mode=SHARED),
        ev(REQUEST, 12, "T", "L", mode=EXCLUSIVE, stack=["upgrade"]),
    ]
    res = run(events, {"1:L": RWLOCK})
    assert not res.anomalies
    h = res.gens[1].holders["T"]["L"]
    assert h.mode == EXCLUSIVE


def test_rwlock_upgrade_with_other_reader_blocks():
    events = starts(["A", "B"]) + [
        ev(REQUEST, 10, "A", "L", mode=SHARED),
        ev(GRANT, 11, "A", "L", mode=SHARED),
        ev(REQUEST, 12, "B", "L", mode=SHARED),
        ev(GRANT, 13, "B", "L", mode=SHARED),
        ev(REQUEST, 14, "A", "L", mode=EXCLUSIVE, stack=["up"]),
    ]
    res = run(events, {"1:L": RWLOCK})
    assert any(a.kind == A_UPGRADE_WHILE_READER for a in res.anomalies)
    assert "A" in res.gens[1].waiting["L"]


def test_condition_wait_releases_then_reacquires():
    events = starts(["P", "W"]) + [
        ev(REQUEST, 10, "P", "L", stack=["lock"]),
        ev(GRANT, 11, "P", "L"),
        ev(COND_WAIT, 12, "P", "L", cond="c", stack=["await"]),
        # W can now acquire, notify and release
        ev(REQUEST, 20, "W", "L"),
        ev(GRANT, 21, "W", "L"),
        ev(COND_NOTIFY, 22, "W", "L", cond="c"),
        ev(RELEASE, 23, "W", "L"),
        ev(COND_GRANT, 24, "P", "L"),
        ev(RELEASE, 25, "P", "L"),
    ]
    res = run(events)
    assert not res.anomalies
    mid = res.frames[4]
    # while parked P holds nothing
    gs = res.gens[1]
    assert "P" in gs.cond_waiting or "P" in gs.holders  # final reacquired
    final_holders = res.gens[1].holders.get("P", {})
    assert "L" not in final_holders
    # while waiting, no wait-for edge toward W (lock was released)
    from app.engine import ReplayEngine
    prefix = ReplayEngine().run(events[:5])
    assert "L" not in prefix.gens[1].holders.get("P", {})
    assert "P" in prefix.gens[1].cond_waiting


def test_condition_grant_without_wait_is_anomaly():
    events = starts(["P"]) + [
        ev(COND_GRANT, 10, "P", "L"),
    ]
    res = run(events)
    assert any(a.kind == A_COND_GRANT_NO_WAIT for a in res.anomalies)


def test_condition_wait_without_hold_is_anomaly():
    events = starts(["P"]) + [
        ev(COND_WAIT, 10, "P", "L", cond="c"),
    ]
    res = run(events)
    assert any(a.kind == A_COND_WAIT_NO_LOCK for a in res.anomalies)


def test_timeout_cancel_revokes_wait():
    events = starts(["H", "W"]) + [
        ev(REQUEST, 10, "H", "L"),
        ev(GRANT, 11, "H", "L"),
        ev(REQUEST, 12, "W", "L", timeout=True, stack=["try"]),
        ev(CANCEL, 30, "W", "L"),
    ]
    res = run(events)
    assert not res.anomalies
    assert "W" not in res.gens[1].waiting.get("L", {})
    assert not cycle_sigs(res)


def test_timeout_cancel_nothing_is_anomaly():
    events = starts(["W"]) + [ev(CANCEL, 5, "W", "L")]
    res = run(events)
    assert any(a.kind == A_CANCEL_NOTHING for a in res.anomalies)


def test_condition_wait_timeout_cancel():
    events = starts(["P"]) + [
        ev(REQUEST, 10, "P", "L"),
        ev(GRANT, 11, "P", "L"),
        ev(COND_WAIT, 12, "P", "L", cond="c", timeout=True),
        ev(CANCEL, 30, "P", "L"),
    ]
    res = run(events)
    assert "P" not in res.gens[1].cond_waiting


def test_thread_exit_revokes_wait_and_flags_leak():
    events = starts(["H", "D"]) + [
        ev(REQUEST, 10, "H", "L"),
        ev(GRANT, 11, "H", "L"),
        ev(REQUEST, 12, "D", "L2"),
        ev(GRANT, 13, "D", "L2"),
        ev(REQUEST, 14, "D", "L"),
        ev(THREAD_END, 30, "D"),
    ]
    res = run(events)
    assert any(a.kind == A_THREAD_LEAK and a.lock == "L2"
               for a in res.anomalies)
    # wait revoked, no fake release of L2 either (hold edge survives low conf)
    assert "D" not in res.gens[1].waiting.get("L", {})
    assert "L2" in res.gens[1].holders.get("D", {})


def test_abba_deadlock_cycle_with_stacks():
    res = run(abba())
    sigs = cycle_sigs(res)
    assert len(sigs) == 1
    last_cycles = list(res.cycles_at.values())[-1]
    c = last_cycles[0]
    assert set(c["thread_sequence"]) == {"T1", "T2"}
    assert set(c["lock_sequence"]) == {"A", "B"}
    assert c["confidence"] == FIRM
    waits = {e["waiter"]: e for e in c["edges"]}
    assert waits["T1"]["wait_stack"] == ["a:2"]
    edge_b = next(e for e in c["edges"] if e["lock"] == "B")
    assert edge_b["hold_stack"] == ["b:1"]
    assert waits["T1"]["wait_request_event"]
    assert waits["T2"]["hold_grant_event"]


def test_two_cycles_coexist():
    events = abba("T1", "T2", "A", "B")
    events += abba("T3", "T4", "C", "D", t=2000)
    res = run(events)
    assert len(cycle_sigs(res)) == 2


def test_near_miss_no_cycle_when_not_overlapping():
    events = starts(["T7", "T8"]) + [
        ev(REQUEST, 100, "T7", "A"), ev(GRANT, 101, "T7", "A"),
        ev(REQUEST, 102, "T7", "B"), ev(GRANT, 103, "T7", "B"),
        ev(RELEASE, 110, "T7", "B"), ev(RELEASE, 111, "T7", "A"),
        ev(REQUEST, 120, "T8", "B"), ev(GRANT, 121, "T8", "B"),
        ev(REQUEST, 122, "T8", "A"), ev(GRANT, 123, "T8", "A"),
        ev(RELEASE, 130, "T8", "A"), ev(RELEASE, 131, "T8", "B"),
    ]
    res = run(events)
    assert not cycle_sigs(res)
    from app.views import near_misses
    nms = near_misses(res)
    assert len(nms) == 1
    assert nms[0]["classification"] == "near_miss"
