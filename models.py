"""事件与锁类型的基础定义。事件以 dict 形式在系统内流转，便于 JSON 序列化。"""

KINDS = {
    "thread_start",  # 线程开始
    "thread_end",    # 线程结束（撤销其等待）
    "req",           # acquire 请求（mode: write/read/upgrade，可带 timeout）
    "acq",           # acquire 成功
    "rel",           # 释放
    "wait",          # condition wait（暂时释放锁，等待 cond）
    "notify",        # condition notify（mode=all 表示 notify_all）
    "timeout",       # 超时撤销等待
}

LOCK_TYPES = {"mutex", "recursive", "rwlock"}

EVENT_FIELDS = ("gen", "kind", "ts", "thread", "lock", "mode", "cond", "timeout", "arrival")


def make_event(gen, kind, ts, thread=None, lock=None, mode=None,
               cond=None, timeout=None, arrival=0, evid=None):
    return {
        "id": evid,
        "gen": gen,
        "kind": kind,
        "ts": float(ts),
        "thread": thread,
        "lock": lock,
        "mode": mode,
        "cond": cond,
        "timeout": timeout,
        "arrival": arrival,
    }


def lock_key(gen, addr):
    """同一地址在不同采集代次（进程重启）属于不同锁。"""
    return (gen, addr)
