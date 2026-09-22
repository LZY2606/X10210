"""内置回放样例：覆盖各类锁序与异常情形。"""
from __future__ import annotations

from typing import Any, Dict, List


def deadlock_sample() -> List[Dict[str, Any]]:
    """经典 A-B / B-A 死锁。"""
    return [
        {"ts": 1, "kind": "thread_start", "thread": "T1"},
        {"ts": 2, "kind": "thread_start", "thread": "T2"},
        {"ts": 10, "kind": "acq_request", "thread": "T1", "lock": "A",
         "stack": ["t1.c:10 main"]},
        {"ts": 11, "kind": "acq_granted", "thread": "T1", "lock": "A",
         "stack": ["t1.c:10 main"]},
        {"ts": 20, "kind": "acq_request", "thread": "T2", "lock": "B",
         "stack": ["t2.c:20 worker"]},
        {"ts": 21, "kind": "acq_granted", "thread": "T2", "lock": "B",
         "stack": ["t2.c:20 worker"]},
        {"ts": 30, "kind": "acq_request", "thread": "T1", "lock": "B",
         "stack": ["t1.c:30 inner", "t1.c:10 main"]},
        {"ts": 40, "kind": "acq_request", "thread": "T2", "lock": "A",
         "stack": ["t2.c:40 inner", "t2.c:20 worker"]},
    ]


def reentrant_sample() -> List[Dict[str, Any]]:
    return [
        {"ts": 1, "kind": "thread_start", "thread": "T1"},
        {"ts": 2, "kind": "acq_request", "thread": "T1", "lock": "L",
         "stack": ["r.c:1"]},
        {"ts": 3, "kind": "acq_granted", "thread": "T1", "lock": "L",
         "stack": ["r.c:1"]},
        {"ts": 4, "kind": "acq_request", "thread": "T1", "lock": "L",
         "stack": ["r.c:2 recursive"]},
        {"ts": 5, "kind": "acq_request", "thread": "T2", "lock": "L",
         "stack": ["r.c:9 other"]},
        {"ts": 6, "kind": "release", "thread": "T1", "lock": "L",
         "stack": ["r.c:2 recursive"]},
        {"ts": 7, "kind": "release", "thread": "T1", "lock": "L",
         "stack": ["r.c:1"]},
        {"ts": 8, "kind": "acq_granted", "thread": "T2", "lock": "L",
         "stack": ["r.c:9 other"]},
        {"ts": 9, "kind": "thread_end", "thread": "T1"},
    ]


def rwlock_upgrade_sample() -> List[Dict[str, Any]]:
    """两个读者都持有 S 后同时请求 X：升级死锁。"""
    return [
        {"ts": 1, "kind": "thread_start", "thread": "R1"},
        {"ts": 2, "kind": "thread_start", "thread": "R2"},
        {"ts": 10, "kind": "acq_request", "thread": "R1", "lock": "RW",
         "mode": "S", "stack": ["rw.c:10 read"]},
        {"ts": 11, "kind": "acq_granted", "thread": "R1", "lock": "RW",
         "mode": "S", "stack": ["rw.c:10 read"]},
        {"ts": 12, "kind": "acq_request", "thread": "R2", "lock": "RW",
         "mode": "S", "stack": ["rw.c:12 read"]},
        {"ts": 13, "kind": "acq_granted", "thread": "R2", "lock": "RW",
         "mode": "S", "stack": ["rw.c:12 read"]},
        {"ts": 20, "kind": "acq_request", "thread": "R1", "lock": "RW",
         "mode": "X", "stack": ["rw.c:20 upgrade"]},
        {"ts": 21, "kind": "acq_request", "thread": "R2", "lock": "RW",
         "mode": "X", "stack": ["rw.c:21 upgrade"]},
    ]


def condition_sample() -> List[Dict[str, Any]]:
    """cond_wait 暂时释放锁、notify 唤醒后重新竞争。"""
    return [
        {"ts": 1, "kind": "thread_start", "thread": "W"},
        {"ts": 2, "kind": "thread_start", "thread": "S"},
        {"ts": 10, "kind": "acq_request", "thread": "W", "lock": "M",
         "stack": ["c.c:10"]},
        {"ts": 11, "kind": "acq_granted", "thread": "W", "lock": "M",
         "stack": ["c.c:10"]},
        {"ts": 20, "kind": "cond_wait", "thread": "W", "lock": "M",
         "gen_from": "CV", "stack": ["c.c:20 wait"]},
        {"ts": 30, "kind": "acq_request", "thread": "S", "lock": "M",
         "stack": ["c.c:30 signaler"]},
        {"ts": 31, "kind": "acq_granted", "thread": "S", "lock": "M",
         "stack": ["c.c:30 signaler"]},
        {"ts": 40, "kind": "cond_notify", "thread": "S", "lock": "M",
         "gen_from": "CV", "stack": ["c.c:40 notify"]},
        {"ts": 41, "kind": "release", "thread": "S", "lock": "M",
         "stack": ["c.c:41"]},
        {"ts": 50, "kind": "acq_granted", "thread": "W", "lock": "M",
         "stack": ["c.c:50 wake"]},
    ]


def timeout_cancel_sample() -> List[Dict[str, Any]]:
    return [
        {"ts": 1, "kind": "thread_start", "thread": "H"},
        {"ts": 2, "kind": "thread_start", "thread": "W"},
        {"ts": 10, "kind": "acq_request", "thread": "H", "lock": "L",
         "stack": ["t.c:10"]},
        {"ts": 11, "kind": "acq_granted", "thread": "H", "lock": "L",
         "stack": ["t.c:10"]},
        {"ts": 20, "kind": "acq_request", "thread": "W", "lock": "L",
         "timeout": 100, "stack": ["t.c:20 wait"]},
        {"ts": 121, "kind": "acq_timeout", "thread": "W", "lock": "L",
         "stack": ["t.c:121 timeout"]},
        {"ts": 130, "kind": "release", "thread": "H", "lock": "L",
         "stack": ["t.c:130"]},
        {"ts": 131, "kind": "thread_end", "thread": "W"},
    ]


def address_reuse_sample() -> List[Dict[str, Any]]:
    """进程重启后同一地址 0x7f0 属于不同代次。"""
    return [
        {"gen": "g1", "ts": 1, "kind": "thread_start", "thread": "T1"},
        {"gen": "g1", "ts": 2, "kind": "acq_request", "thread": "T1",
         "lock": "0x7f0", "stack": ["p1.c:2"]},
        {"gen": "g1", "ts": 3, "kind": "acq_granted", "thread": "T1",
         "lock": "0x7f0", "stack": ["p1.c:2"]},
        {"gen": "g2", "ts": 100, "kind": "thread_start", "thread": "T2"},
        {"gen": "g2", "ts": 101, "kind": "acq_request", "thread": "T2",
         "lock": "0x7f0", "stack": ["p2.c:101"]},
        {"gen": "g2", "ts": 102, "kind": "acq_granted", "thread": "T2",
         "lock": "0x7f0", "stack": ["p2.c:101"]},
        {"gen": "g2", "ts": 103, "kind": "release", "thread": "T2",
         "lock": "0x7f0", "stack": ["p2.c:103"]},
    ]


def missing_release_sample() -> List[Dict[str, Any]]:
    """缺 release：线程结束仍持锁，系统不得凭空补释放。"""
    return [
        {"ts": 1, "kind": "thread_start", "thread": "T1"},
        {"ts": 2, "kind": "thread_start", "thread": "T2"},
        {"ts": 10, "kind": "acq_request", "thread": "T1", "lock": "M",
         "stack": ["m.c:10"]},
        {"ts": 11, "kind": "acq_granted", "thread": "T1", "lock": "M",
         "stack": ["m.c:10"]},
        {"ts": 20, "kind": "acq_request", "thread": "T2", "lock": "M",
         "stack": ["m.c:20 block"]},
        {"ts": 30, "kind": "thread_end", "thread": "T1"},
    ]


def out_of_order_sample() -> List[Dict[str, Any]]:
    """乱序到达：seq 大的 granted 先到库，仍按 ts 重建。"""
    return [
        {"seq": 1, "ts": 1, "kind": "thread_start", "thread": "T1"},
        {"seq": 3, "ts": 11, "kind": "acq_granted", "thread": "T1",
         "lock": "L", "stack": ["o.c:11"]},
        {"seq": 2, "ts": 10, "kind": "acq_request", "thread": "T1",
         "lock": "L", "stack": ["o.c:10"]},
        {"seq": 4, "ts": 20, "kind": "release", "thread": "T1",
         "lock": "L", "stack": ["o.c:20"]},
    ]


def multi_cycle_sample() -> List[Dict[str, Any]]:
    """两组并存的死锁环。"""
    base = deadlock_sample()
    base += [
        {"ts": 3, "kind": "thread_start", "thread": "T3"},
        {"ts": 4, "kind": "thread_start", "thread": "T4"},
        {"ts": 50, "kind": "acq_request", "thread": "T3", "lock": "C",
         "stack": ["m.c:50"]},
        {"ts": 51, "kind": "acq_granted", "thread": "T3", "lock": "C",
         "stack": ["m.c:50"]},
        {"ts": 60, "kind": "acq_request", "thread": "T4", "lock": "D",
         "stack": ["m.c:60"]},
        {"ts": 61, "kind": "acq_granted", "thread": "T4", "lock": "D",
         "stack": ["m.c:60"]},
        {"ts": 70, "kind": "acq_request", "thread": "T3", "lock": "D",
         "stack": ["m.c:70"]},
        {"ts": 80, "kind": "acq_request", "thread": "T4", "lock": "C",
         "stack": ["m.c:80"]},
    ]
    return base


SAMPLES: Dict[str, Dict[str, Any]] = {
    "deadlock": {"label": "经典 AB 死锁", "events": deadlock_sample()},
    "reentrant": {"label": "可重入锁", "events": reentrant_sample()},
    "rwlock_upgrade": {"label": "读写锁升级死锁", "events": rwlock_upgrade_sample()},
    "condition": {"label": "Condition 等待/通知", "events": condition_sample()},
    "timeout": {"label": "超时取消", "events": timeout_cancel_sample()},
    "reuse": {"label": "地址复用（跨代次）", "events": address_reuse_sample()},
    "missing": {"label": "缺失 release", "events": missing_release_sample()},
    "out_of_order": {"label": "乱序到达", "events": out_of_order_sample()},
    "multi_cycle": {"label": "多环并存", "events": multi_cycle_sample()},
}
