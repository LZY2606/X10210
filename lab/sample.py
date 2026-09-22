"""内置演示轨迹。"""
from __future__ import annotations

from typing import Any, Dict, List


def demo_events() -> List[Dict[str, Any]]:
    return [
        # 可重入互斥 0xM
        {"id": "m-start", "type": "thread_start", "thread": "T1", "ts": 1},
        {"id": "m1", "type": "request", "thread": "T1", "lock": "0xM", "ts": 2,
         "stack": ["main", "enter_cache"]},
        {"id": "m2", "type": "acquired", "thread": "T1", "lock": "0xM", "ts": 3,
         "stack": ["main", "enter_cache"]},
        {"id": "m3", "type": "request", "thread": "T1", "lock": "0xM", "ts": 4,
         "stack": ["enter_cache", "cache_get"]},
        {"id": "m4", "type": "acquired", "thread": "T1", "lock": "0xM", "ts": 5,
         "stack": ["enter_cache", "cache_get"]},
        {"id": "m5", "type": "release", "thread": "T1", "lock": "0xM", "ts": 6},
        {"id": "m6", "type": "release", "thread": "T1", "lock": "0xM", "ts": 7},

        # 读写锁升级（声明 0xRW 为 rwlock）：R1 与 R2 共享读，同时升级写 -> 死锁
        {"id": "rw-start1", "type": "thread_start", "thread": "R1", "ts": 10},
        {"id": "rw-start2", "type": "thread_start", "thread": "R2", "ts": 10},
        {"id": "rw1", "type": "request", "thread": "R1", "lock": "0xRW",
         "mode": "read", "ts": 11, "stack": ["reader1", "snapshot"]},
        {"id": "rw2", "type": "acquired", "thread": "R1", "lock": "0xRW",
         "mode": "read", "ts": 12},
        {"id": "rw3", "type": "request", "thread": "R2", "lock": "0xRW",
         "mode": "read", "ts": 13, "stack": ["reader2", "snapshot"]},
        {"id": "rw4", "type": "acquired", "thread": "R2", "lock": "0xRW",
         "mode": "read", "ts": 14},
        {"id": "rw5", "type": "request", "thread": "R1", "lock": "0xRW",
         "mode": "write", "ts": 15, "stack": ["reader1", "upgrade_commit"]},
        {"id": "rw6", "type": "request", "thread": "R2", "lock": "0xRW",
         "mode": "write", "ts": 16, "stack": ["reader2", "upgrade_commit"]},

        # ABBA 经典死锁
        {"id": "ab-start1", "type": "thread_start", "thread": "A", "ts": 20},
        {"id": "ab-start2", "type": "thread_start", "thread": "B", "ts": 20},
        {"id": "ab1", "type": "request", "thread": "A", "lock": "0xA",
         "ts": 21, "stack": ["workerA", "step1"]},
        {"id": "ab2", "type": "acquired", "thread": "A", "lock": "0xA", "ts": 22},
        {"id": "ab3", "type": "request", "thread": "B", "lock": "0xB",
         "ts": 23, "stack": ["workerB", "step1"]},
        {"id": "ab4", "type": "acquired", "thread": "B", "lock": "0xB", "ts": 24},
        {"id": "ab5", "type": "request", "thread": "A", "lock": "0xB",
         "ts": 25, "stack": ["workerA", "step2_wait_B"]},
        {"id": "ab6", "type": "request", "thread": "B", "lock": "0xA",
         "ts": 26, "stack": ["workerB", "step2_wait_A"]},

        # condition：T3 持锁进入 wait，T4 通知后 T3 重新竞争成功
        {"id": "c-start1", "type": "thread_start", "thread": "C1", "ts": 30},
        {"id": "c-start2", "type": "thread_start", "thread": "C2", "ts": 30},
        {"id": "c1", "type": "request", "thread": "C1", "lock": "0xC",
         "cond": "cv", "ts": 31, "stack": ["consumer", "wait_queue"]},
        {"id": "c2", "type": "acquired", "thread": "C1", "lock": "0xC", "ts": 32},
        {"id": "c3", "type": "cond_wait", "thread": "C1", "lock": "0xC",
         "cond": "cv", "ts": 33, "stack": ["consumer", "paused"]},
        {"id": "c4", "type": "request", "thread": "C2", "lock": "0xC", "ts": 34,
         "stack": ["producer", "lock_for_signal"]},
        {"id": "c5", "type": "acquired", "thread": "C2", "lock": "0xC", "ts": 35},
        {"id": "c6", "type": "cond_notify", "thread": "C2", "lock": "0xC",
         "cond": "cv", "ts": 36},
        {"id": "c7", "type": "release", "thread": "C2", "lock": "0xC", "ts": 37},
        {"id": "c8", "type": "acquired", "thread": "C1", "lock": "0xC", "ts": 38,
         "stack": ["consumer", "resumed"]},
        {"id": "c9", "type": "release", "thread": "C1", "lock": "0xC", "ts": 39},

        # 超时取消：T5 等待 0xT，超时撤销，不产生环
        {"id": "t-start", "type": "thread_start", "thread": "T5", "ts": 40},
        {"id": "t-start2", "type": "thread_start", "thread": "T6", "ts": 40},
        {"id": "t1", "type": "request", "thread": "T6", "lock": "0xT", "ts": 41,
         "stack": ["owner6"]},
        {"id": "t2", "type": "acquired", "thread": "T6", "lock": "0xT", "ts": 42},
        {"id": "t3", "type": "request", "thread": "T5", "lock": "0xT", "ts": 43,
         "stack": ["waiter5", "timed_lock"]},
        {"id": "t4", "type": "timeout", "thread": "T5", "lock": "0xT", "ts": 44},

        # 重启：同一地址 0xA 在新代次是另一把锁，不与上面的环相连
        {"id": "r", "type": "restart", "thread": "kernel", "ts": 50},
        {"id": "p-start", "type": "thread_start", "thread": "P1", "ts": 51},
        {"id": "p1", "type": "request", "thread": "P1", "lock": "0xA", "ts": 52,
         "stack": ["new_process"]},
        {"id": "p2", "type": "acquired", "thread": "P1", "lock": "0xA", "ts": 53},
        {"id": "p3", "type": "release", "thread": "P1", "lock": "0xA", "ts": 54},
    ]


def demo_declarations() -> Dict[str, str]:
    return {"0xRW": "rwlock"}
