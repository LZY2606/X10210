"""锁序解剖室核心场景测试。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from locklab.engine import Engine, CERTAIN, UNCERTAIN
from locklab.minimize import minimize

_id = [0]


def ev(ts, thread, type_, lock=None, gen=1, seq=None, extra=None):
    _id[0] += 1
    return {"id": _id[0], "generation": gen, "seq": seq if seq is not None else _id[0],
            "ts": ts, "thread": thread, "lock": lock, "type": type_, "extra": extra or {}}


def acq(ts, th, lock, mode=None, gen=1):
    extra = {"mode": mode} if mode else {}
    return [ev(ts, th, "acquire_request", lock, gen, extra=extra),
            ev(ts + 0.5, th, "acquire_success", lock, gen, extra=extra)]


def test_recursive_lock_depth_and_no_self_cycle():
    events = [ev(1, "T1", "thread_start")]
    events += acq(2, "T1", "0xA")
    events += acq(4, "T1", "0xA")
    events += acq(6, "T1", "0xB")
    engine = Engine(events, {"1:0xA": "recursive"})
    state = engine.state_at(100)
    holds = {h["lock"]: h for h in state.held_graph()}
    assert holds["1:0xA"]["depth"] == 2
    assert holds["1:0xB"]["depth"] == 1
    assert engine.cycles_at(100) == []
    # 释放一次后仍持有
    events.append(ev(8, "T1", "release", "0xA"))
    engine.reload(events)
    state = engine.state_at(100)
    holds = {h["lock"]: h for h in state.held_graph()}
    assert holds["1:0xA"]["depth"] == 1


def test_rwlock_shared_and_upgrade_deadlock():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xR", mode="shared")
    events += acq(4, "T2", "0xR", mode="shared")
    engine = Engine(events, {"1:0xR": "rwlock"})
    state = engine.state_at(100)
    # 共享持有互不冲突
    assert state.edges() == []
    # 双方请求升级 -> 死锁环
    events.append(ev(6, "T1", "acquire_request", "0xR", extra={"mode": "exclusive"}))
    events.append(ev(7, "T2", "acquire_request", "0xR", extra={"mode": "exclusive"}))
    engine.reload(events)
    cycles = engine.cycles_at(100)
    assert len(cycles) == 1
    cyc = cycles[0]
    assert sorted(cyc["threads"]) == ["T1", "T2"]
    assert all(e["wait_stack"][0]["kind"] == "upgrade" for e in cyc["edges"])
    # 证据包含持有栈与等待栈及事件 id
    for e in cyc["edges"]:
        assert e["wait_event"] and e["hold_event"]
        assert e["held_stack"] and e["wait_stack"]


def test_condition_wait_notify_reacquire():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xM")
    events.append(ev(4, "T1", "cond_wait_start", "0xM", extra={"cond": "0xQ"}))
    engine = Engine(events)
    state = engine.state_at(100)
    # wait 临时释放互斥锁
    assert state.held_graph() == []
    assert state.waits()[0]["kind"] == "cond"
    assert state.edges() == []
    # T2 拿到锁并 notify，T1 转为重新竞争
    events += acq(5, "T2", "0xM")
    events.append(ev(7, "T2", "notify", "0xM", extra={"cond": "0xQ"}))
    engine.reload(events)
    state = engine.state_at(100)
    assert state.waits()[0]["kind"] == "reacquire"
    edges = state.edges()
    assert len(edges) == 1 and edges[0]["from"] == "T1" and edges[0]["to"] == "T2"
    # T2 释放，T1 重新获取成功，无等待无环
    events.append(ev(8, "T2", "release", "0xM"))
    events.append(ev(9, "T1", "cond_wait_end", "0xM", extra={"cond": "0xQ"}))
    engine.reload(events)
    state = engine.state_at(100)
    assert state.edges() == []
    assert engine.cycles_at(100) == []
    assert state.held_graph()[0]["thread"] == "T1"


def test_timeout_cancels_wait():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xA")
    events.append(ev(4, "T2", "acquire_request", "0xA"))
    engine = Engine(events)
    assert len(engine.state_at(100).edges()) == 1
    events.append(ev(6, "T2", "timeout", "0xA"))
    engine.reload(events)
    assert engine.state_at(100).edges() == []
    assert engine.cycles_at(100) == []


def test_thread_exit_cancels_wait_but_not_release():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xA")
    events.append(ev(4, "T2", "acquire_request", "0xA"))
    events.append(ev(5, "T2", "thread_end"))
    engine = Engine(events)
    assert engine.state_at(100).edges() == []
    # 持有者退出：不凭空补 release，持有保留但降级
    events.append(ev(6, "T1", "thread_end"))
    engine.reload(events)
    state = engine.state_at(100)
    holds = state.held_graph()
    assert len(holds) == 1
    assert holds[0]["leaked"] is True
    assert holds[0]["confidence"] == UNCERTAIN


def test_address_reuse_across_generations():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xA", gen=1)
    # 进程重启后的新代次，同地址是另一把锁
    events.append(ev(4, "T2", "acquire_request", "0xA", gen=2))
    engine = Engine(events)
    state = engine.state_at(100)
    assert state.edges() == []
    assert engine.cycles_at(100) == []
    assert state.waits()[0]["lock"] == "2:0xA"


def test_missing_release_and_missing_request():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    # 缺 acquire_request 的 success：不补请求，只降级
    events.append(ev(2, "T1", "acquire_success", "0xA"))
    # 缺 release：T2 的等待边置信度为不确定
    events.append(ev(4, "T2", "acquire_request", "0xA"))
    engine = Engine(events)
    state = engine.state_at(100)
    edges = state.edges()
    assert len(edges) == 1
    assert edges[0]["confidence"] == UNCERTAIN
    assert any("without matching request" in a["message"] for a in state.anomalies)
    # 多余的 release 不产生任何状态
    events.append(ev(6, "T1", "release", "0xA"))
    events.append(ev(7, "T1", "release", "0xA"))
    engine.reload(events)
    state = engine.state_at(100)
    assert state.held_graph() == []
    assert any("release without matching hold" in a["message"] for a in state.anomalies)


def test_out_of_order_arrival_keeps_candidate_graph():
    events = [ev(1, "T1", "thread_start"), ev(1, "T2", "thread_start")]
    events += acq(2, "T1", "0xA")
    # T2 的事件时间戳倒挂
    events.append(ev(9, "T2", "acquire_request", "0xB"))
    events.append(ev(3, "T2", "acquire_success", "0xB"))
    events.append(ev(8, "T1", "acquire_request", "0xB"))
    engine = Engine(events)
    state = engine.state_at(100)
    assert any("out-of-order" in a["message"] for a in state.anomalies)
    edges = state.edges()
    assert len(edges) == 1
    assert edges[0]["confidence"] == UNCERTAIN


def test_multiple_cycles_coexist():
    events = [ev(1, t, "thread_start") for t in ("T1", "T2", "T3", "T4")]
    events += acq(2, "T1", "0xA")
    events += acq(3, "T2", "0xB")
    events += acq(4, "T3", "0xC")
    events += acq(5, "T4", "0xD")
    events.append(ev(6, "T1", "acquire_request", "0xB"))
    events.append(ev(7, "T2", "acquire_request", "0xA"))
    events.append(ev(8, "T3", "acquire_request", "0xD"))
    events.append(ev(9, "T4", "acquire_request", "0xC"))
    engine = Engine(events)
    cycles = engine.cycles_at(100)
    assert len(cycles) == 2
    thread_sets = {tuple(sorted(c["threads"])) for c in cycles}
    assert thread_sets == {("T1", "T2"), ("T3", "T4")}


def test_streaming_append_only_recomputes_tail():
    events = [ev(1, "T1", "thread_start")]
    for i in range(40):
        events += acq(2 + i * 2, "T1", "0x%02x" % i)
        events.append(ev(3 + i * 2, "T1", "release", "0x%02x" % i))
    engine = Engine(events)
    engine.state_at(float("inf"))
    mark = engine.replay_count
    engine.state_at(50.0)
    delta_before = engine.replay_count - mark
    before = engine.replay_count
    # 流式追加新事件
    new_events = events + acq(200, "T1", "0xZ") + [ev(201, "T2", "acquire_request", "0xZ")]
    engine.reload(new_events)
    # 追加前的时段：快照未失效，重放量与追加前完全一致
    engine.state_at(50.0)
    assert engine.replay_count - before == delta_before
    engine.state_at(float("inf"))
    # 只重算受影响时间段（尾部），而非全部 120+ 事件
    assert engine.replay_count - before <= len(new_events) - len(events) + Engine.SNAP_EVERY


def test_minimize_keeps_cycle_and_condition_semantics():
    events = [ev(1, t, "thread_start") for t in ("T1", "T2", "T3")]
    events += acq(2, "T1", "0xA")
    events += acq(4, "T2", "0xB")
    # 无关噪声：T3 完整 condition 交互
    events += acq(6, "T3", "0xM")
    events.append(ev(8, "T3", "cond_wait_start", "0xM", extra={"cond": "0xQ"}))
    events.append(ev(9, "T1", "notify", "0xM", extra={"cond": "0xQ"}))
    events.append(ev(10, "T3", "cond_wait_end", "0xM", extra={"cond": "0xQ"}))
    events.append(ev(11, "T3", "release", "0xM"))
    # 死锁
    events.append(ev(12, "T1", "acquire_request", "0xB"))
    events.append(ev(13, "T2", "acquire_request", "0xA"))
    engine = Engine(events)
    cycles = engine.cycles_at(100)
    assert len(cycles) == 1
    kept = minimize(events, {}, 100, cycles[0]["signature"])
    assert len(kept) < len(events)
    # 缩减后仍复现同一环
    engine2 = Engine(kept)
    cycles2 = engine2.cycles_at(100)
    assert len(cycles2) == 1
    assert tuple(sorted(cycles2[0]["signature"])) == tuple(sorted(cycles[0]["signature"]))
    # cond 配对不被拆散
    types = [e["type"] for e in kept]
    assert ("cond_wait_start" in types) == ("cond_wait_end" in types)
