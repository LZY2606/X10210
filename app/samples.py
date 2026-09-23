"""内置演示轨迹。"""
from __future__ import annotations


def abba():
    """经典 ABBA 死锁。"""
    return [
        {"type": "thread_start", "thread": "T1", "ts": 1, "stack": ["main"]},
        {"type": "thread_start", "thread": "T2", "ts": 2, "stack": ["main"]},
        {"type": "acquire_req", "thread": "T1", "lock": "L1", "ts": 3,
         "stack": ["T1.work", "lock(L1)"]},
        {"type": "acquired", "thread": "T1", "lock": "L1", "ts": 4,
         "stack": ["T1.work", "lock(L1)"]},
        {"type": "acquire_req", "thread": "T2", "lock": "L2", "ts": 5,
         "stack": ["T2.run", "lock(L2)"]},
        {"type": "acquired", "thread": "T2", "lock": "L2", "ts": 6,
         "stack": ["T2.run", "lock(L2)"]},
        {"type": "acquire_req", "thread": "T1", "lock": "L2", "ts": 7,
         "stack": ["T1.work", "nested(L2)"]},
        {"type": "acquire_req", "thread": "T2", "lock": "L1", "ts": 8,
         "stack": ["T2.run", "nested(L1)"]},
    ]


def recursive():
    return [
        {"type": "thread_start", "thread": "T1", "ts": 1},
        {"type": "acquire_req", "thread": "T1", "lock": "RM", "ts": 2,
         "stack": ["T1.enter", "recursive_lock"]},
        {"type": "acquired", "thread": "T1", "lock": "RM", "ts": 3,
         "stack": ["T1.enter", "recursive_lock"]},
        {"type": "acquire_req", "thread": "T1", "lock": "RM", "ts": 4,
         "stack": ["T1.dfs", "recursive_lock"]},
        {"type": "acquired", "thread": "T1", "lock": "RM", "ts": 5,
         "stack": ["T1.dfs", "recursive_lock"]},
        {"type": "release", "thread": "T1", "lock": "RM", "ts": 6, "stack": ["T1.dfs"]},
        {"type": "release", "thread": "T1", "lock": "RM", "ts": 7, "stack": ["T1.enter"]},
        {"type": "thread_end", "thread": "T1", "ts": 8},
    ]


def rw_upgrade():
    return [
        {"type": "thread_start", "thread": "R1", "ts": 1},
        {"type": "thread_start", "thread": "R2", "ts": 2},
        {"type": "acquire_req", "thread": "R1", "lock": "RW", "mode": "read", "ts": 3,
         "stack": ["R1.scan"]},
        {"type": "acquired", "thread": "R1", "lock": "RW", "mode": "read", "ts": 4,
         "stack": ["R1.scan"]},
        {"type": "acquire_req", "thread": "R2", "lock": "RW", "mode": "read", "ts": 5,
         "stack": ["R2.scan"]},
        {"type": "acquired", "thread": "R2", "lock": "RW", "mode": "read", "ts": 6,
         "stack": ["R2.scan"]},
        {"type": "acquire_req", "thread": "R1", "lock": "RW", "mode": "upgrade", "ts": 7,
         "stack": ["R1.commit", "upgrade"]},
        {"type": "acquire_req", "thread": "R2", "lock": "RW", "mode": "upgrade", "ts": 8,
         "stack": ["R2.commit", "upgrade"]},
    ]


def condition_demo():
    return [
        {"type": "thread_start", "thread": "P", "ts": 1},
        {"type": "thread_start", "thread": "C", "ts": 2},
        {"type": "acquire_req", "thread": "C", "lock": "M", "ts": 3, "stack": ["consumer"]},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 4, "stack": ["consumer"]},
        {"type": "cond_wait", "thread": "C", "lock": "M", "cond": "cv", "ts": 5,
         "stack": ["consumer.await"]},
        {"type": "acquire_req", "thread": "P", "lock": "M", "ts": 6, "stack": ["producer"]},
        {"type": "acquired", "thread": "P", "lock": "M", "ts": 7, "stack": ["producer"]},
        {"type": "cond_notify", "thread": "P", "lock": "M", "cond": "cv", "ts": 8,
         "stack": ["producer.signal"]},
        {"type": "release", "thread": "P", "lock": "M", "ts": 9, "stack": ["producer.put"]},
        {"type": "cond_acquired", "thread": "C", "lock": "M", "cond": "cv", "ts": 10,
         "stack": ["consumer.wake"]},
        {"type": "release", "thread": "C", "lock": "M", "ts": 11, "stack": ["consumer.get"]},
        {"type": "thread_end", "thread": "P", "ts": 12},
        {"type": "thread_end", "thread": "C", "ts": 13},
    ]


def timeout_cancel():
    return [
        {"type": "thread_start", "thread": "H", "ts": 1},
        {"type": "thread_start", "thread": "W", "ts": 2},
        {"type": "acquire_req", "thread": "H", "lock": "L", "ts": 3, "stack": ["H.hold"]},
        {"type": "acquired", "thread": "H", "lock": "L", "ts": 4, "stack": ["H.hold"]},
        {"type": "acquire_req", "thread": "W", "lock": "L", "ts": 5, "stack": ["W.trylock"]},
        {"type": "cancelled", "thread": "W", "lock": "L", "ts": 6, "stack": ["W.timeout"]},
        {"type": "release", "thread": "H", "lock": "L", "ts": 7, "stack": ["H.done"]},
        {"type": "thread_end", "thread": "H", "ts": 8},
        {"type": "thread_end", "thread": "W", "ts": 9},
    ]


def address_reuse():
    """同锁地址 0x7f00 跨进程重启，属于不同代次。"""
    return [
        {"type": "thread_start", "gen": "boot1", "thread": "A", "ts": 1},
        {"type": "acquire_req", "gen": "boot1", "thread": "A", "lock": "0x7f00", "ts": 2,
         "stack": ["boot1.lock"]},
        {"type": "acquired", "gen": "boot1", "thread": "A", "lock": "0x7f00", "ts": 3,
         "stack": ["boot1.lock"]},
        # 没有 release：进程崩溃重启
        {"type": "thread_end", "gen": "boot1", "thread": "A", "ts": 4},
        {"type": "thread_start", "gen": "boot2", "thread": "B", "ts": 5},
        {"type": "acquire_req", "gen": "boot2", "thread": "B", "lock": "0x7f00", "ts": 6,
         "stack": ["boot2.fresh_lock"]},
        {"type": "acquired", "gen": "boot2", "thread": "B", "lock": "0x7f00", "ts": 7,
         "stack": ["boot2.fresh_lock"]},
        {"type": "release", "gen": "boot2", "thread": "B", "lock": "0x7f00", "ts": 8,
         "stack": ["boot2.fresh_lock"]},
        {"type": "thread_end", "gen": "boot2", "thread": "B", "ts": 9},
    ]


def missing_release():
    return [
        {"type": "thread_start", "thread": "X", "ts": 1},
        {"type": "thread_start", "thread": "Y", "ts": 2},
        {"type": "acquire_req", "thread": "X", "lock": "L", "ts": 3, "stack": ["X.grab"]},
        {"type": "acquired", "thread": "X", "lock": "L", "ts": 4, "stack": ["X.grab"]},
        {"type": "thread_end", "thread": "X", "ts": 5},
        {"type": "acquire_req", "thread": "Y", "lock": "L", "ts": 6, "stack": ["Y.grab"]},
    ]


def multi_cycle():
    return [
        {"type": "thread_start", "thread": "A", "ts": 1},
        {"type": "thread_start", "thread": "B", "ts": 2},
        {"type": "thread_start", "thread": "C", "ts": 3},
        {"type": "thread_start", "thread": "D", "ts": 4},
        {"type": "acquire_req", "thread": "A", "lock": "L1", "ts": 5, "stack": ["A>1"]},
        {"type": "acquired", "thread": "A", "lock": "L1", "ts": 6, "stack": ["A>1"]},
        {"type": "acquire_req", "thread": "B", "lock": "L2", "ts": 7, "stack": ["B>2"]},
        {"type": "acquired", "thread": "B", "lock": "L2", "ts": 8, "stack": ["B>2"]},
        {"type": "acquire_req", "thread": "C", "lock": "L3", "ts": 9, "stack": ["C>3"]},
        {"type": "acquired", "thread": "C", "lock": "L3", "ts": 10, "stack": ["C>3"]},
        {"type": "acquire_req", "thread": "D", "lock": "L4", "ts": 11, "stack": ["D>4"]},
        {"type": "acquired", "thread": "D", "lock": "L4", "ts": 12, "stack": ["D>4"]},
        {"type": "acquire_req", "thread": "A", "lock": "L2", "ts": 13, "stack": ["A>2"]},
        {"type": "acquire_req", "thread": "B", "lock": "L1", "ts": 14, "stack": ["B>1"]},
        {"type": "acquire_req", "thread": "C", "lock": "L4", "ts": 15, "stack": ["C>4"]},
        {"type": "acquire_req", "thread": "D", "lock": "L3", "ts": 16, "stack": ["D>3"]},
    ]


SAMPLES = {
    "abba": ("ABBA 死锁", abba),
    "recursive": ("递归锁重入", recursive),
    "rw_upgrade": ("读写锁共享与升级", rw_upgrade),
    "condition": ("Condition wait/notify", condition_demo),
    "timeout": ("超时取消等待", timeout_cancel),
    "address_reuse": ("地址复用（跨代次）", address_reuse),
    "missing_release": ("缺 release（不补全）", missing_release),
    "multi_cycle": ("多环并存", multi_cycle),
}
