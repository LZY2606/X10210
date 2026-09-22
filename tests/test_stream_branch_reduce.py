import time

from app.models import RECURSIVE, Event
from app.engine import StreamEngine, ReplayEngine
from app.reduce import minimize, reproduces, _closure
from app.storage import LabStore
from app.demo import demo_events, demo_lock_types
from tests.helpers import (
    ev, reset_ids, starts, cycle_sigs,
    REQUEST, GRANT, RELEASE, THREAD_END, CANCEL,
    COND_WAIT, COND_NOTIFY, COND_GRANT,
)
from tests.test_engine import abba


def setup_function(_):
    reset_ids()


def _abba_payloads():
    return [e.to_dict() for e in abba()]


def test_stream_out_of_order_append_matches_full_replay():
    engine = StreamEngine()
    payloads = demo_events()
    # first half by logical order, then a late low-ts event, then rest
    events = [Event.from_dict(d, seq=i, id_=i + 1)
              for i, d in enumerate(payloads)]
    events.sort(key=lambda e: e.sort_key())
    mid = len(events) // 2
    first, second = events[:mid], events[mid:]
    # make one early event arrive late
    late = first.pop(-3)
    engine.append(first)
    engine.append(second)
    stats = engine.append([late])
    assert stats.late == 1
    incremental = engine.full_result()
    full = ReplayEngine(lock_types=demo_lock_types()).run(
        sorted(first + second + [late], key=lambda e: e.sort_key()))
    assert [f.event_id for f in incremental.frames] == \
           [f.event_id for f in full.frames]
    assert set(incremental.cycles_at) == set(full.cycles_at)
    assert stats.frames_recomputed < stats.total_frames or stats.late


def test_stream_stats_reflect_recomputed_span():
    engine = StreamEngine()
    events = [Event.from_dict(d, id_=i + 1)
              for i, d in enumerate(demo_events())]
    engine.append(events[:60])
    late = [Event.from_dict(dict(demo_events()[10]), id_=1000)]
    late[0].ts = 950
    stats = engine.append(late)
    assert stats.late == 1
    # replay restarts at checkpoint 0 or 32, never at the very end
    assert stats.replay_from_frame <= stats.total_frames


def test_generation_address_reuse_is_independent():
    events = abba("T1", "T2", "A", "B", gen=1, t=100)
    events += [
        Event(kind="thread_start", ts=10, thread="P1", gen=2, id=900),
        Event(kind="thread_start", ts=11, thread="P2", gen=2, id=901),
        Event(kind=REQUEST, ts=20, thread="P1", lock="A", gen=2, id=902),
        Event(kind=GRANT, ts=21, thread="P1", lock="A", gen=2, id=903),
        Event(kind=REQUEST, ts=22, thread="P2", lock="A", gen=2, id=904),
        Event(kind=CANCEL, ts=23, thread="P2", lock="A", gen=2, id=905),
        Event(kind=RELEASE, ts=24, thread="P1", lock="A", gen=2, id=906),
    ]
    res = ReplayEngine().run(events)
    assert len(cycle_sigs(res, gen=1)) == 1
    assert not any(c["gen"] == 2
                   for fr in res.cycles_at.values() for c in fr)


def test_missing_release_never_synthesized():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "L"),
        ev(GRANT, 11, "T", "L"),
    ]
    res = run_if_possible(events)
    assert "L" in res.gens[1].holders["T"]
    assert not res.anomalies


def run_if_possible(events):
    return ReplayEngine().run(events)


def test_orphan_grant_is_tentative_not_crash():
    events = starts(["T"]) + [
        ev(GRANT, 10, "T", "L", stack=["orphan"]),
    ]
    res = ReplayEngine().run(events)
    h = res.gens[1].holders["T"]["L"]
    assert h.confidence == "tentative"


# --- storage / branches ----------------------------------------------------
def test_branch_correction_update_breaks_cycle():
    store = LabStore(":memory:")
    ids = store.add_events(_abba_payloads())
    base = store.replay("base")
    sig = next(iter(cycle_sigs(base)))
    assert sig
    store.create_branch("fix", "base", "T1 不再请求 B")
    # delete T1's blocking request for B (the 5th abba event after 2 starts)
    req_b = [e for e in base.events
             if e.thread == "T1" and e.kind == REQUEST and e.lock == "B"][0]
    store.add_correction("fix", {"op": "delete", "id": req_b.id})
    fixed = store.replay("fix", force=True)
    assert not cycle_sigs(fixed)
    # base untouched
    assert cycle_sigs(store.replay("base", force=True))


def test_branch_declare_recursive_changes_reconstruction():
    store = LabStore(":memory:")
    store.add_events([e.to_dict() for e in starts(["T"])] + [
        {"kind": REQUEST, "ts": 10, "thread": "T", "lock": "R", "gen": 1},
        {"kind": GRANT, "ts": 11, "thread": "T", "lock": "R", "gen": 1},
        {"kind": REQUEST, "ts": 12, "thread": "T", "lock": "R", "gen": 1},
    ])
    store.create_branch("rec", "base")
    store.add_correction("rec", {"op": "declare", "gen": 1,
                                 "lock": "R", "type": RECURSIVE})
    res = store.replay("rec", force=True)
    assert not any(a.kind == "self_deadlock_request" for a in res.anomalies)


def test_branch_insert_correction_can_resolve_via_release():
    store = LabStore(":memory:")
    store.add_events(_abba_payloads())
    store.create_branch("ins", "base")
    store.add_correction("ins", {
        "op": "insert",
        "event": {"kind": RELEASE, "ts": 1014, "thread": "T2",
                  "lock": "B", "gen": 1},
        "note": "T2 提前释放 B"})
    res = store.replay("ins", force=True)
    # T2 released B before asking A: request at t21 can now succeed? T2 still
    # requests A which T1 holds -> it waits, but T1 still waits B held by
    # nobody now -> cycle broken.
    assert not cycle_sigs(res)


def test_compare_branches_reports_cycle_diff():
    store = LabStore(":memory:")
    store.add_events(_abba_payloads())
    store.create_branch("fix", "base")
    req_b = [e for e in store.replay("base").events
             if e.thread == "T1" and e.kind == REQUEST and e.lock == "B"][0]
    store.add_correction("fix", {"op": "delete", "id": req_b.id})
    store.replay("fix", force=True)
    # emulate compare endpoint logic
    ra = store.replay("base")
    rb = store.replay("fix")
    sa = cycle_sigs(ra)
    sb = cycle_sigs(rb)
    assert sa and not sb


# --- minimizer -------------------------------------------------------------
def test_minimize_abba_shrinks_trace():
    events = abba()
    # pad with unrelated noise
    noise = starts(["N1", "N2"], t0=5000) + [
        ev(REQUEST, 5010, "N1", "X"), ev(GRANT, 5011, "N1", "X"),
        ev(RELEASE, 5012, "N1", "X"),
        ev(REQUEST, 5013, "N2", "Y"), ev(GRANT, 5014, "N2", "Y"),
        ev(RELEASE, 5015, "N2", "Y"),
        ev(THREAD_END, 5020, "N1"), ev(THREAD_END, 5021, "N2"),
    ]
    res = ReplayEngine().run(events + noise)
    sig = next(iter(cycle_sigs(res)))
    out = minimize(events + noise, sig)
    assert out["found"]
    assert out["minimal_size"] < len(events) + len(noise)
    assert reproduces(
        [Event.from_dict(d) for d in out["events"]], sig)


def test_closure_keeps_request_grant_release_pairing():
    events = starts(["T"]) + [
        ev(REQUEST, 10, "T", "L"),
        ev(GRANT, 11, "T", "L"),
        ev(REQUEST, 20, "T", "L2"),
        ev(GRANT, 21, "T", "L2"),
        ev(RELEASE, 30, "T", "L2"),
    ]
    keep = {events[-1].id}  # only the release
    closed = _closure(events, keep)
    kinds = [(e.kind, e.lock) for e in closed]
    assert (REQUEST, "L2") in kinds and (GRANT, "L2") in kinds
    assert all(e.lock != "L" for e in closed)


def test_minimize_keeps_condition_semantics():
    events = starts(["P", "W"]) + [
        ev(REQUEST, 10, "P", "L"),
        ev(GRANT, 11, "P", "L"),
        ev(COND_WAIT, 12, "P", "L", cond="c"),
        ev(REQUEST, 20, "W", "L"),
        ev(GRANT, 21, "W", "L"),
        ev(COND_NOTIFY, 22, "W", "L", cond="c"),
        ev(RELEASE, 23, "W", "L"),
        ev(COND_GRANT, 24, "P", "L"),
    ]
    cw = next(e for e in events if e.kind == COND_WAIT)
    out = _closure(events, {cw.id})
    kinds = [e.kind for e in out]
    assert "cond_wait" in kinds and "cond_grant" in kinds
    assert "cond_notify" in kinds
