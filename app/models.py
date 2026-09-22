"""事件模型与规范化。

一个原始事件（raw event）永远不被原地修改；分支上的“校正”是一张
event_id -> 覆盖字段 的映射。重建引擎只看到规范化之后的有效事件。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

EVENT_KINDS = {
    "thread_start",
    "thread_end",
    "acquire_request",
    "acquired",
    "released",
    "cond_wait",
    "cond_notify",
    "notify_all",
    "timeout",
}

LOCK_KINDS = {"mutex", "reentrant", "rwlock"}
LOCK_MODES = {"exclusive", "shared"}


class ValidationError(ValueError):
    pass


def normalize_event(raw: Dict[str, Any], seq: int) -> Dict[str, Any]:
    """把外部 JSON 行规范化为内部事件 dict。

    seq 是该代次内的采集序号（按到达顺序），没有显式 id 时用它生成稳定 id。
    """
    if not isinstance(raw, dict):
        raise ValidationError("event must be an object")

    kind = raw.get("kind")
    if kind not in EVENT_KINDS:
        raise ValidationError("unknown event kind: %r" % (kind,))

    event_id = raw.get("id") or "e%d" % seq

    ts = raw.get("ts")
    if ts is None:
        raise ValidationError("event %s missing ts" % event_id)
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        raise ValidationError("event %s has non-numeric ts" % event_id)

    def _opt_str(name: str) -> Optional[str]:
        value = raw.get(name)
        return None if value is None else str(value)

    tid = _opt_str("tid")
    lock = _opt_str("lock")
    cond = _opt_str("cond")

    if kind in ("thread_start", "thread_end"):
        if not tid:
            raise ValidationError("event %s requires tid" % event_id)
    elif kind in ("acquire_request", "acquired", "released"):
        if not tid or not lock:
            raise ValidationError("event %s requires tid and lock" % event_id)
    else:  # condition / timeout
        if not tid:
            raise ValidationError("event %s requires tid" % event_id)

    mode = raw.get("mode", "exclusive")
    if mode not in LOCK_MODES:
        raise ValidationError("event %s bad mode: %r" % (event_id, mode))

    stack = raw.get("stack") or []
    if not isinstance(stack, list) or not all(isinstance(f, str) for f in stack):
        raise ValidationError("event %s stack must be list[str]" % event_id)

    deadline = raw.get("deadline")
    if deadline is not None:
        try:
            deadline = float(deadline)
        except (TypeError, ValueError):
            raise ValidationError("event %s bad deadline" % event_id)

    event = {
        "id": str(event_id),
        "seq": seq,
        "kind": kind,
        "ts": ts,
        "tid": tid,
        "lock": lock,
        "cond": cond,
        "mode": mode,
        "stack": list(stack),
        "deadline": deadline,
        "raw_kind": kind,
    }
    return event


def apply_fix(event: Dict[str, Any], fix: Dict[str, Any]) -> Dict[str, Any]:
    """在事件上叠加一张校正覆盖表，返回新事件（不修改入参）。"""
    out = dict(event)
    for field in ("kind", "ts", "tid", "lock", "cond", "mode", "deadline", "stack"):
        if field in fix and fix[field] is not None:
            out[field] = fix[field]
    out["stack"] = list(out.get("stack") or [])
    if out.get("deadline") is not None:
        out["deadline"] = float(out["deadline"])
    out["ts"] = float(out["ts"])
    if out["kind"] not in EVENT_KINDS:
        raise ValidationError("fix produces unknown kind: %r" % out["kind"])
    if out["kind"] in ("acquire_request", "acquired", "released") and not out["lock"]:
        raise ValidationError("fix removes lock from lock event %s" % out["id"])
    return out
