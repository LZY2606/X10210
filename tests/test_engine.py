"""引擎场景测试：递归锁 / 读写升级 / condition / 超时 / 线程退出 /
地址复用 / 缺 release / 乱序 / 多环 / 流式增量 / 最小化缩减。"""
import json

from locklab.engine import CANDIDATE, Incremental, replay
from locklab.minimize import minimize, pairing_valid


def ev(i, type_, ts, thread=None, lock=None, epoch=1, **kw):
    e = {"id": i, "epoch": epoch, "seq": i, "type": type_, "ts": ts,
         "thread": thread, "lock": lock}
    e.update(kw)
    return e


def test_recursive_lock():
    events = [
        ev(1, "acquire_request", 1, "T1", "L", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "L", wait_id="w1"),
        ev(3, "acquire_request", 3, "T1", "L", wait_id="w2"),
        ev(4, "acquire", 4, "T1", "L", wait_id="w2"),
        ev(5, "release", 5, "T1", "L"),
    ]
    snap = replay(events)
    assert snap["cycles"] == []
    assert snap["wait_for"] == []
    hold = [h for h in snap["holds"] if h["lock"] == "1:L"]
    assert len(hold) == 1 and hold[0]["count"] == 1  # 仍持有一层


def test_rwlock_upgrade_deadlock():
    decls = {(1, "L"): "rwlock"}
    events = [
        ev(1, "acquire_request", 1, "T1", "L", kind="read", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "L", kind="read", wait_id="w1"),
        ev(3, "acquire_request", 3, "T2", "L", kind="read", wait_id="w2"),
        ev(4, "acquire", 4, "T2", "L", kind="read", wait_id="w2"),
        ev(5, "acquire_request", 5, "T1", "L", kind="write", wait_id="w3"),  # 升级
        ev(6, "acquire_request", 6, "T2", "L", kind="write", wait_id="w4"),  # 升级
    ]
    snap = replay(events, declarations=decls)
    assert len(snap["cycles"]) == 1
    cyc = snap["cycles"][0]
    assert set(cyc["threads"]) == {"T1", "T2"}
    # 证据包含持有栈与等待栈，而非只有锁名
    for th in ("T1", "T2"):
        assert cyc["detail"][th]["waiting_for"]["lock"] == "1:L"
        assert any(h["lock"] == "1:L" and h["kind"] == "read"
                   for h in cyc["detail"][th]["holding"])


def test_condition_wait_notify():
    events = [
        ev(1, "acquire_request", 1, "T1", "M", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "M", wait_id="w1"),
        ev(3, "cond_wait", 3, "T1", "M", cond="C"),
        ev(4, "acquire_request", 4, "T2", "M", wait_id="w2"),
        ev(5, "acquire", 5, "T2", "M", wait_id="w2"),
    ]
    snap = replay(events, t=3)
    # wait 暂时释放了 M
    assert not [h for h in snap["holds"] if h["thread"] == "T1"]
    assert any(w["type"] == "cond" and w["cond"] == "C" for w in snap["waits"])
    snap = replay(events, t=5)
    assert [h for h in snap["holds"] if h["thread"] == "T2"]  # T2 拿到了 M

    events += [
        ev(6, "cond_notify", 6, "T2", cond="C"),
        ev(7, "release", 7, "T2", "M"),
        ev(8, "acquire", 8, "T1", "M"),
    ]
    snap = replay(events, t=6)
    # notify 后 T1 重新竞争 M，等待边指向持有者 T2
    assert any(e["from"] == "T1" and e["to"] == "T2" and e["lock"] == "1:M"
               for e in snap["wait_for"])
    snap = replay(events)
    hold = [h for h in snap["holds"] if h["thread"] == "T1"]
    assert len(hold) == 1 and hold[0]["count"] == 1  # 重新竞争成功并恢复计数


def test_timeout_cancel():
    events = [
        ev(1, "acquire_request", 1, "T1", "L", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "L", wait_id="w1"),
        ev(3, "acquire_request", 3, "T2", "L", wait_id="w2", timeout_ms=5),
        ev(4, "release", 20, "T1", "L"),
    ]
    snap = replay(events, t=4)
    assert any(e["from"] == "T2" for e in snap["wait_for"])  # 超时前在等待
    snap = replay(events)
    assert snap["wait_for"] == []  # 超时撤销等待
    assert any(a["kind"] == "timeout_cancel" for a in snap["anomalies"])
    assert snap["cycles"] == []


def test_thread_exit_cancels_wait():
    events = [
        ev(1, "acquire_request", 1, "T1", "L", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "L", wait_id="w1"),
        ev(3, "acquire_request", 3, "T2", "L", wait_id="w2"),
        ev(4, "thread_end", 4, "T2"),
    ]
    snap = replay(events)
    assert snap["wait_for"] == []
    assert not [w for w in snap["waits"] if w["thread"] == "T2"]
    assert any(a["kind"] == "end_cancels_wait" for a in snap["anomalies"])


def test_address_reuse_across_epochs():
    events = [
        ev(1, "acquire_request", 1, "T1", "A", epoch=1, wait_id="w1"),
        ev(2, "acquire", 2, "T1", "A", epoch=1, wait_id="w1"),
        # 进程重启：代次 2 中地址 A 是另一把锁
        ev(3, "acquire_request", 3, "U1", "A", epoch=2, wait_id="w2"),
        ev(4, "acquire", 4, "U1", "A", epoch=2, wait_id="w2"),
        ev(5, "acquire_request", 5, "U2", "A", epoch=2, wait_id="w3"),
    ]
    snap = replay(events)
    locks = {h["lock"] for h in snap["holds"]}
    assert locks == {"1:A", "2:A"}  # 同地址不同代次 = 不同锁
    assert all(not (e["from"] == "U2" and e["to"] == "T1") for e in snap["wait_for"])
    assert any(e["from"] == "U2" and e["to"] == "U1" for e in snap["wait_for"])
    assert snap["cycles"] == []


def test_missing_release_no_fabrication():
    events = [
        ev(1, "acquire_request", 1, "T1", "L", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "L", wait_id="w1"),
        ev(3, "release", 3, "T2", "L"),  # 无对应持有的 release：只记异常
    ]
    snap = replay(events)
    holds = [h for h in snap["holds"] if h["thread"] == "T1"]
    assert len(holds) == 1 and holds[0]["open"]  # 缺 release：持有保持打开
    assert any(a["kind"] == "release_without_hold" for a in snap["anomalies"])
    # 引擎不凭空补 release：事件序列原样保留
    assert [e["type"] for e in events].count("release") == 1


def test_out_of_order_candidate_graph():
    events = [
        ev(1, "acquire_request", 10, "T1", "L", wait_id="w1"),
        ev(2, "acquire", 5, "T1", "L", wait_id="w1"),   # ts 倒退：乱序到达
        ev(3, "acquire_request", 8, "T2", "L", wait_id="w2"),
    ]
    snap = replay(events)
    assert any(a["kind"] == "out_of_order" for a in snap["anomalies"])
    # 原始顺序保留（按 seq 回放），但派生边为候选置信
    hold = [h for h in snap["holds"] if h["thread"] == "T1"]
    assert hold and hold[0]["confidence"] == CANDIDATE
    edge = [e for e in snap["wait_for"] if e["from"] == "T2"]
    assert edge and edge[0]["confidence"] == CANDIDATE


def test_multiple_cycles_coexist():
    events = []
    i = 0
    for t1, t2, la, lb in (("T1", "T2", "A", "B"), ("T3", "T4", "C", "D")):
        events += [
            ev(i := i + 1, "acquire_request", i, t1, la, wait_id=f"w{i}"),
            ev(i := i + 1, "acquire", i, t1, la, wait_id=f"w{i - 1}"),
            ev(i := i + 1, "acquire_request", i, t2, lb, wait_id=f"w{i}"),
            ev(i := i + 1, "acquire", i, t2, lb, wait_id=f"w{i - 1}"),
            ev(i := i + 1, "acquire_request", i, t1, lb, wait_id=f"w{i}"),
            ev(i := i + 1, "acquire_request", i, t2, la, wait_id=f"w{i}"),
        ]
    snap = replay(events)
    sets = {frozenset(c["threads"]) for c in snap["cycles"]}
    assert {frozenset({"T1", "T2"}), frozenset({"T3", "T4"})} <= sets


def _filler_events(n, start=0, ts0=100):
    """无环填充事件：成对的请求/获取/释放。"""
    out = []
    for k in range(n):
        i = start + k * 3
        out += [
            ev(i + 1, "acquire_request", ts0 + i, "F" + str(k % 3), f"F{k % 5}",
               wait_id=f"f{i}"),
            ev(i + 2, "acquire", ts0 + i + 1, "F" + str(k % 3), f"F{k % 5}",
               wait_id=f"f{i}"),
            ev(i + 3, "release", ts0 + i + 2, "F" + str(k % 3), f"F{k % 5}"),
        ]
    return out


def test_incremental_streaming_tail_only():
    events = _filler_events(8)  # 24 个事件
    events += [ev(901, "acquire_request", 901, "F0", "Z", wait_id="z1"),
               ev(902, "acquire", 902, "F0", "Z", wait_id="z1")]  # 末尾留下持有
    inc = Incremental(segment=8)
    inc.append(events[:16])
    assert inc.stats["segments_recomputed"] == 2
    assert inc.stats["events_replayed"] == 16
    inc.append(events[16:20])
    assert inc.stats["segments_recomputed"] == 2  # 不足一段，不新增检查点
    assert inc.stats["events_replayed"] == 20
    inc.append(events[20:])
    assert inc.stats["segments_recomputed"] == 3  # 只重算受影响尾段
    # 追加 6 个事件只触发了尾段（最近检查点之后 10 个）的重放，而非全部 26 个
    assert inc.stats["events_replayed"] == 30
    # 增量结果与全量回放一致
    full = replay(events)
    got = inc.snapshot()
    assert json.dumps(got, sort_keys=True) == json.dumps(full, sort_keys=True)


def test_minimize_keeps_pairing_and_cycle():
    deadlock = [
        ev(1, "acquire_request", 1, "T1", "A", wait_id="w1"),
        ev(2, "acquire", 2, "T1", "A", wait_id="w1"),
        ev(3, "acquire_request", 3, "T2", "B", wait_id="w2"),
        ev(4, "acquire", 4, "T2", "B", wait_id="w2"),
        ev(5, "acquire_request", 5, "T1", "B", wait_id="w3"),
        ev(6, "acquire_request", 6, "T2", "A", wait_id="w4"),
    ]
    events = deadlock + _filler_events(12, start=100)
    snap = replay(events)
    assert any(set(c["threads"]) == {"T1", "T2"} for c in snap["cycles"])

    reduced = minimize(events, ["T1", "T2"])
    assert len(reduced) < len(events)
    assert pairing_valid(reduced)
    snap2 = replay(reduced)
    assert any(set(c["threads"]) == {"T1", "T2"} for c in snap2["cycles"])
    # 填充事件全部可删；持有与等待的核心事件保留（请求事件可被异常容错吸收）
    ids = {e["id"] for e in reduced}
    assert ids <= {1, 2, 3, 4, 5, 6}
    assert {2, 4, 5, 6} <= ids


def test_minimize_preserves_cond_semantics():
    # cond_wait 后 release 而无重新 acquire：配对非法
    bad = [
        ev(1, "acquire", 1, "T1", "M"),
        ev(2, "cond_wait", 2, "T1", "M", cond="C"),
        ev(3, "release", 3, "T1", "M"),
    ]
    assert not pairing_valid(bad)
    # wait -> 重新 acquire -> release 合法
    good = [ev(1, "acquire", 1, "T1", "M"),
            ev(2, "cond_wait", 2, "T1", "M", cond="C"),
            ev(3, "acquire", 3, "T1", "M"),
            ev(4, "release", 4, "T1", "M")]
    assert pairing_valid(good)
