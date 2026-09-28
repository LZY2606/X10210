"""Raw event model. Events are immutable observations, never invented by us."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

KINDS = {
    "req",        # acquire request issued
    "acq",        # acquire succeeded (granted)
    "rel",        # release
    "tstart",     # thread started
    "tend",       # thread ended
    "cwait",      # condition.wait: releases held lock, parks until notify
    "cnotify",    # condition notify / notify_all: wakes waiter(s) to reacquire
}
KIND_ALIASES = {
    "acquire_request": "req", "request": "req", "lock": "req",
    "acquire": "acq", "acquired": "acq", "grant": "acq",
    "release": "rel", "released": "rel", "unlock": "rel",
    "thread_start": "tstart", "start": "tstart",
    "thread_end": "tend", "end": "tend", "exit": "tend",
    "wait": "cwait", "cond_wait": "cwait", "condwait": "cwait",
    "notify": "cnotify", "signal": "cnotify", "notify_all": "cnotify",
    "broadcast": "cnotify",
}
MODES = ("X", "S")


@dataclass
class RawEvent:
    eid: int
    kind: str
    ts: int                      # microsecond timestamp
    tid: str
    lock: Optional[str] = None
    gen: int = 0                 # collection generation (pid/boot era)
    mode: str = "X"              # X = exclusive, S = shared
    cond: Optional[str] = None
    timeout_us: Optional[int] = None
    count: Optional[int] = None  # notify: how many waiters (None/0 => all)
    stack: Optional[list[str]] = None
    seq: int = 0                 # arrival sequence (raw order within a gen)
    invalid: bool = False        # correction: mark event as ignored
    note: str = ""

    @staticmethod
    def from_dict(d: dict[str, Any], eid: int, seq: int) -> "RawEvent":
        kind = str(d.get("kind", "")).strip()
        kind = KIND_ALIASES.get(kind, kind)
        mode = str(d.get("mode", "X")).upper()
        if mode in ("R", "READ", "SHARED", "SH"):
            mode = "S"
        if mode in ("W", "WRITE", "EXCLUSIVE", "EX"):
            mode = "X"
        return RawEvent(
            eid=int(d.get("eid", eid)),
            kind=kind,
            ts=int(d["ts"]),
            tid=str(d["tid"]),
            lock=(str(d["lock"]) if d.get("lock") is not None else None),
            gen=int(d.get("gen", 0)),
            mode=mode if mode in MODES else "X",
            cond=(str(d["cond"]) if d.get("cond") is not None else None),
            timeout_us=(int(d["timeout_us"]) if d.get("timeout_us") is not None else None),
            count=(int(d["count"]) if d.get("count") is not None else None),
            stack=list(d["stack"]) if d.get("stack") else None,
            seq=int(d.get("seq", seq)),
            invalid=bool(d.get("invalid", False)),
            note=str(d.get("note", "")),
        )

    def key(self) -> tuple[int, str]:
        return (self.gen, self.lock or "")

    def to_dict(self) -> dict[str, Any]:
        return {
            "eid": self.eid, "kind": self.kind, "ts": self.ts, "tid": self.tid,
            "lock": self.lock, "gen": self.gen, "mode": self.mode,
            "cond": self.cond, "timeout_us": self.timeout_us, "count": self.count,
            "stack": self.stack, "seq": self.seq, "invalid": self.invalid,
            "note": self.note,
        }
