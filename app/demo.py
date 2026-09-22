"""Built-in demonstration trace.

Generation 1 contains several independent scenarios placed on different
locks/threads:

* classic ABBA deadlock on 0x7f01 (T1,T2) -> a real cycle
* second simultaneous ABBA on 0x7f03/0x7f04 (T3,T4) -> two cycles at once
* recursive lock on R1 (no deadlock)
* rwlock read->write upgrade success (RW)
* rwlock upgrade that blocks between two readers (RW2)
* condition wait / notify / re-acquire (CV with cond c1)
* timed-out request cancellation (T5 on 0x7f09)
* thread exiting while holding a lock -> leak anomaly (T6 on 0x7f0a)
* near-miss: AB and BA that never overlap (T7,T8 on 0x7f0b/0x7f0c)
* orphan grant (missing request) on 0x7f0d
* missing release: T9 keeps 0x7f0e forever

Generation 2 reuses address 0x7f01 after a process restart to demonstrate
generation separation.
"""
from __future__ import annotations

from typing import Dict, List

from .models import MUTEX, RECURSIVE, RWLOCK


def demo_events() -> List[dict]:
    ev: List[dict] = []

    def add(ts, kind, thread, **kw):
        d = {"ts": ts, "kind": kind, "thread": thread, "gen": 1}
        d.update(kw)
        ev.append(d)

    # --- threads ---
    for i, t in enumerate(["T1", "T2", "T3", "T4", "TRec", "TRw",
                           "TRwA", "TRwB", "TCVp", "TCVw", "T5", "T6",
                           "T7", "T8", "T9"]):
        add(100 + i, "thread_start", t)

    # --- lock acquired early; its release is ingested late (out of order) ---
    add(900, "request", "T9", lock="0x7f00", stack=["late:acq"])
    add(901, "grant", "T9", lock="0x7f00")

    # --- classic ABBA deadlock (0x7f01 A, 0x7f02 B) ---
    add(1000, "request", "T1", lock="0x7f01", mode="X",
        stack=["main:32", "Service.a:55"])
    add(1001, "grant", "T1", lock="0x7f01", mode="X")
    add(1010, "request", "T2", lock="0x7f02", mode="X",
        stack=["worker:11", "Service.b:78"])
    add(1011, "grant", "T2", lock="0x7f02", mode="X")
    add(1020, "request", "T1", lock="0x7f02", mode="X",
        stack=["Service.a:60", "Store.write:120"])
    add(1021, "request", "T2", lock="0x7f01", mode="X",
        stack=["Service.b:83", "Store.write:120"])

    # --- second simultaneous ABBA (0x7f03 C, 0x7f04 D) ---
    add(1030, "request", "T3", lock="0x7f03", stack=["job:1"])
    add(1031, "grant", "T3", lock="0x7f03")
    add(1032, "request", "T4", lock="0x7f04", stack=["job:9"])
    add(1033, "grant", "T4", lock="0x7f04")
    add(1040, "request", "T3", lock="0x7f04", stack=["job:2"])
    add(1041, "request", "T4", lock="0x7f03", stack=["job:10"])

    # --- recursive lock R1 ---
    add(1100, "request", "TRec", lock="R1", stack=["rec:1"])
    add(1101, "grant", "TRec", lock="R1")
    add(1102, "request", "TRec", lock="R1", stack=["rec:2"])
    add(1103, "request", "TRec", lock="R1", stack=["rec:3"])
    add(1110, "release", "TRec", lock="R1")
    add(1111, "release", "TRec", lock="R1")
    add(1112, "release", "TRec", lock="R1")

    # --- rwlock upgrade: sole reader upgrades cleanly ---
    add(1200, "request", "TRw", lock="RW", mode="S", stack=["rw:read"])
    add(1201, "grant", "TRw", lock="RW", mode="S")
    add(1210, "request", "TRw", lock="RW", mode="X", stack=["rw:upgrade"])
    add(1211, "release", "TRw", lock="RW")

    # --- rwlock upgrade blocked by another reader ---
    add(1250, "request", "TRwA", lock="RW2", mode="S", stack=["rw2:Aread"])
    add(1251, "grant", "TRwA", lock="RW2", mode="S")
    add(1252, "request", "TRwB", lock="RW2", mode="S", stack=["rw2:Bread"])
    add(1253, "grant", "TRwB", lock="RW2", mode="S")
    add(1260, "request", "TRwA", lock="RW2", mode="X",
        stack=["rw2:Aupgrade"])

    # --- condition variable: producer releases CV1 while waiting ---
    add(1300, "request", "TCVp", lock="CV1", stack=["cv:lock"])
    add(1301, "grant", "TCVp", lock="CV1")
    add(1310, "cond_wait", "TCVp", lock="CV1", cond="c1",
        stack=["cv:await"], timeout=False)
    add(1320, "request", "TCVw", lock="CV1", stack=["cv:other-lock"])
    add(1321, "grant", "TCVw", lock="CV1")
    add(1330, "cond_notify", "TCVw", lock="CV1", cond="c1")
    add(1340, "release", "TCVw", lock="CV1")
    add(1341, "cond_grant", "TCVp", lock="CV1")
    add(1342, "release", "TCVp", lock="CV1")

    # --- timeout cancellation ---
    add(1400, "request", "T9", lock="0x7f09", stack=["tmo:holder"])
    add(1401, "grant", "T9", lock="0x7f09")
    add(1410, "request", "T5", lock="0x7f09", timeout=True,
        stack=["tmo:try"])
    add(1460, "cancel", "T5", lock="0x7f09")

    # --- thread exits holding lock 0x7f0a ---
    add(1500, "request", "T6", lock="0x7f0a", stack=["leak:get"])
    add(1501, "grant", "T6", lock="0x7f0a")
    add(1510, "thread_end", "T6")

    # --- near-miss: AB then BA at disjoint times ---
    add(1600, "request", "T7", lock="0x7f0b", stack=["nm:7.A"])
    add(1601, "grant", "T7", lock="0x7f0b")
    add(1602, "request", "T7", lock="0x7f0c", stack=["nm:7.B"])
    add(1603, "grant", "T7", lock="0x7f0c")
    add(1610, "release", "T7", lock="0x7f0c")
    add(1611, "release", "T7", lock="0x7f0b")
    add(1620, "request", "T8", lock="0x7f0c", stack=["nm:8.B"])
    add(1621, "grant", "T8", lock="0x7f0c")
    add(1622, "request", "T8", lock="0x7f0b", stack=["nm:8.A"])
    add(1623, "grant", "T8", lock="0x7f0b")
    add(1630, "release", "T8", lock="0x7f0b")
    add(1631, "release", "T8", lock="0x7f0c")

    # --- orphan grant (request event missing) ---
    add(1700, "grant", "T9", lock="0x7f0d", stack=["missing:request"])
    add(1701, "release", "T9", lock="0x7f0d")

    # --- missing release: T9 keeps 0x7f0e forever ---
    add(1800, "request", "T9", lock="0x7f0e", stack=["norelease:get"])
    add(1801, "grant", "T9", lock="0x7f0e")

    # --- generation 2: address reuse after restart --------------------
    ev2 = []
    for t in ("P1", "P2"):
        ev2.append({"ts": 50, "kind": "thread_start", "thread": t, "gen": 2})
    ev2 += [
        {"ts": 60, "kind": "request", "thread": "P1", "lock": "0x7f01",
         "gen": 2, "stack": ["restart:p1"]},
        {"ts": 61, "kind": "grant", "thread": "P1", "lock": "0x7f01",
         "gen": 2},
        {"ts": 70, "kind": "request", "thread": "P2", "lock": "0x7f01",
         "gen": 2, "stack": ["restart:p2"]},
        {"ts": 71, "kind": "cancel", "thread": "P2", "lock": "0x7f01",
         "gen": 2},
        {"ts": 80, "kind": "release", "thread": "P1", "lock": "0x7f01",
         "gen": 2},
    ]
    ev.extend(ev2)
    return ev


def demo_lock_types() -> Dict[str, str]:
    return {
        "1:R1": RECURSIVE,
        "1:RW": RWLOCK,
        "1:RW2": RWLOCK,
    }


# One event is intentionally sent "late" (arrives after events with larger
# timestamps) to demonstrate out-of-order handling.
LATE_EVENT = {"ts": 920, "kind": "release", "thread": "T9",
              "lock": "0x7f00", "gen": 1, "stack": ["late:rel"]}
