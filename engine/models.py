"""Canonical event model and normalization.

Events arrive as JSON dictionaries. They keep their raw order within a
collection generation (``gen``). A process restart starts a new generation:
the same lock address in different generations is a different lock.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Canonical event kinds
ACQUIRE_REQUEST = "acquire_request"
ACQUIRE_GRANTED = "acquire_granted"
RELEASE = "release"
THREAD_START = "thread_start"
THREAD_END = "thread_end"
COND_WAIT = "cond_wait"
COND_WAIT_END = "cond_wait_end"
NOTIFY = "notify"
CANCEL = "acquire_cancelled"
RESTART = "restart"

#: accepted external aliases -> canonical kind
KIND_ALIASES = {
    "acquire_request": ACQUIRE_REQUEST,
    "acquire_req": ACQUIRE_REQUEST,
    "request": ACQUIRE_REQUEST,
    "lock_request": ACQUIRE_REQUEST,
    "acquire": ACQUIRE_GRANTED,
    "acquire_granted": ACQUIRE_GRANTED,
    "acquired": ACQUIRE_GRANTED,
    "grant": ACQUIRE_GRANTED,
    "granted": ACQUIRE_GRANTED,
    "lock_acquired": ACQUIRE_GRANTED,
    "release": RELEASE,
    "released": RELEASE,
    "lock_release": RELEASE,
    "unlock": RELEASE,
    "thread_start": THREAD_START,
    "start": THREAD_START,
    "thread_end": THREAD_END,
    "end": THREAD_END,
    "exit": THREAD_END,
    "thread_exit": THREAD_END,
    "cond_wait": COND_WAIT,
    "condition_wait": COND_WAIT,
    "wait": COND_WAIT,
    "cond_wait_end": COND_WAIT_END,
    "condition_wait_end": COND_WAIT_END,
    "wait_end": COND_WAIT_END,
    "notify": NOTIFY,
    "signal": NOTIFY,
    "notify_all": NOTIFY,
    "broadcast": NOTIFY,
    "acquire_cancelled": CANCEL,
    "cancel": CANCEL,
    "cancelled": CANCEL,
    "timeout": CANCEL,
    "acquire_timeout": CANCEL,
    "restart": RESTART,
    "process_restart": RESTART,
}

GEN_KEYS = ("gen", "generation")
TS_KEYS = ("ts", "timestamp", "time")


def normalize_event(raw: Dict[str, Any], idx: int, default_gen: int = 0) -> Dict[str, Any]:
    """Convert a raw JSON event into canonical form.

    ``idx`` is the ingest ordinal. Inserted/corrected events may supply their
    own ``seq``. Order within a generation is by ``(gen, ts_or_None, seq)``
    where equal timestamps fall back to ingest order.
    """
    if not isinstance(raw, dict):
        raise ValueError("event must be an object: %r" % (raw,))
    kind_raw = raw.get("kind") or raw.get("type") or raw.get("event")
    if kind_raw is None:
        raise ValueError("event missing kind/type: %r" % (raw,))
    kind = KIND_ALIASES.get(str(kind_raw).strip().lower())
    if kind is None:
        raise ValueError("unknown event kind %r" % (kind_raw,))

    gen = default_gen
    for key in GEN_KEYS:
        if key in raw and raw[key] is not None:
            gen = int(raw[key])
            break

    ts = None
    for key in TS_KEYS:
        if key in raw and raw[key] is not None:
            ts = float(raw[key])
            break

    tid = raw.get("thread") or raw.get("thread_id") or raw.get("tid")
    lock = raw.get("lock") or raw.get("lock_addr") or raw.get("addr") or raw.get("lock_id")
    cond = raw.get("cond") or raw.get("cond_addr") or raw.get("condition")

    ev: Dict[str, Any] = {
        "id": raw.get("id") or ("e%d" % idx),
        "gen": gen,
        "kind": kind,
        "ts": ts,
        "thread": None if tid is None else str(tid),
        "lock": None if lock is None else str(lock),
        "cond": None if cond is None else str(cond),
        "mode": _norm_mode(raw.get("mode")),
        "stack": _norm_stack(raw.get("stack")),
        "timeout": raw.get("timeout"),
        "seq": int(raw["seq"]) if raw.get("seq") is not None else idx,
        "raw": raw,
    }
    if ev["timeout"] is not None:
        try:
            ev["timeout"] = float(ev["timeout"])
        except (TypeError, ValueError):
            ev["timeout"] = None
    return ev


def _norm_mode(value: Any) -> str:
    if value is None:
        return ""
    mode = str(value).strip().lower()
    if mode in ("r", "read", "shared", "sh", "s"):
        return "read"
    if mode in ("w", "write", "exclusive", "ex", "x"):
        return "write"
    if mode in ("up", "upgrade"):
        return "upgrade"
    return ""


def _norm_stack(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    return [str(frame) for frame in value]


def event_sort_key(ev: Dict[str, Any]):
    return (ev["gen"], ev["ts"] if ev["ts"] is not None else float("inf"), ev["seq"])


def apply_correction(base: Dict[str, Any], corr: Dict[str, Any]) -> Dict[str, Any]:
    """Apply a correction patch to a raw event (shallow merge of known fields)."""
    merged = dict(base)
    for key, value in corr.items():
        if key in ("id", "action"):
            continue
        merged[key] = value
    return merged
