"""锁序解剖室核心引擎。

事件按"采集代次"(generation)保留原始到达顺序；同一锁地址在进程重启后
通过不同 generation 区分。引擎将事件流增量归约为带置信度的候选状态：
可重入锁、读写锁共享持有/升级、condition wait 暂时释放后重新竞争、
超时/取消/线程终止撤销等待。缺事件/乱序只产生软异常与低置信推断，
绝不凭空补 release。
"""
from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

# ---- 锁类型 / 模式 -------------------------------------------------------
MUTEX = "mutex"
RECURSIVE = "recursive"
RWLOCK = "rwlock"
X, S = "X", "S"
LOCK_KINDS = (MUTEX, RECURSIVE, RWLOCK)

SNAPSHOT_EVERY = 128

# 硬异常：代表重建在语义上不合法（例如凭空 release）
HARD_ANOMALIES = frozenset({
    "release_without_hold",
    "release_mode_mismatch",
    "acquire_ok_unknown_request",
    "cond_wait_without_hold",
    "bad_mode",
    "unknown_event",
})
