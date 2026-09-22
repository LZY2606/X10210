from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


ACQUIRE_REQUEST = "acquire_request"
ACQUIRE_SUCCESS = "acquire_success"
RELEASE = "release"
THREAD_START = "thread_start"
THREAD_END = "thread_end"
CONDITION_WAIT = "condition_wait"
CONDITION_NOTIFY = "condition_notify"
TIMEOUT = "timeout"
CANCEL = "cancel"
PROCESS_START = "process_start"
PROCESS_EXIT = "process_exit"
LOCK_TYPE = "lock_type"

EXCLUSIVE = "exclusive"
SHARED = "shared"
UPGRADE = "upgrade"

RECURSIVE_MUTEX = "recursive_mutex"
RWLOCK = "rwlock"
MUTEX = "mutex"


@dataclass
class ModeCount:
    exclusive: int = 0
    shared: Dict[str, int] = field(default_factory=dict)

    def total_shared(self) -> int:
        return sum(self.shared.values())

    def total(self) -> int:
        return self.exclusive + self.total_shared()

    def clone(self) -> "ModeCount":
        return ModeCount(self.exclusive, dict(self.shared))


@dataclass
class LockState:
    id: str
    key: str
    address: str
    generation: str
    lock_type: str = RECURSIVE_MUTEX
    declared_type: bool = False
    exclusive_owner: Optional[str] = None
    mode: ModeCount = field(default_factory=ModeCount)
    waiter_events: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    event_id: Optional[str] = None
    acquired_event: Optional[str] = None
    held_event: Optional[str] = None
    conflicts: List[str] = field(default_factory=list)
    low_confidence: bool = False

    def clone(self) -> "LockState":
        return LockState(
            self.id,
            self.key,
            self.address,
            self.generation,
            self.lock_type,
            self.declared_type,
            self.exclusive_owner,
            self.mode.clone(),
            {key: dict(value) for key, value in self.waiter_events.items()},
            self.event_id,
            self.acquired_event,
            self.held_event,
            list(self.conflicts),
            self.low_confidence,
        )


@dataclass
class ThreadState:
    id: str
    started: bool = False
    ended: bool = False
    start_event: Optional[str] = None
    end_event: Optional[str] = None


@dataclass
class LockHeld:
    lock_id: str
    key: str
    address: str
    generation: str
    mode: str
    count: int
    stack: List[Dict[str, Any]]
    acquire_event_id: str
    event_id: str
    confidence: str = "high"
    status: str = "held"


@dataclass
class AnalysisState:
    locks: Dict[str, LockState] = field(default_factory=dict)
    threads: Dict[str, ThreadState] = field(default_factory=dict)
    held: Dict[str, List[LockHeld]] = field(default_factory=dict)
    lock_waiters: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    pending: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    request_to_waiter: Dict[str, str] = field(default_factory=dict)
    waiter_to_request: Dict[str, str] = field(default_factory=dict)
    condition_waiters: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    active_generation_by_process: Dict[str, str] = field(default_factory=dict)
    cancelled_waiters: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    last_index: int = -1
    last_time: Optional[float] = None
    low_confidence: bool = False

    def clone(self) -> "AnalysisState":
        state = AnalysisState(
            {key: value.clone() for key, value in self.locks.items()},
            {key: ThreadState(value.id, value.started, value.ended, value.start_event, value.end_event) for key, value in self.threads.items()},
            {key: [LockHeld(item.lock_id, item.key, item.address, item.generation, item.mode, item.count, [dict(frame) for frame in item.stack], item.acquire_event_id, item.event_id) for item in stack] for key, stack in self.held.items()},
            {key: {waiter: dict(value) for waiter, value in waiters.items()} for key, value in self.lock_waiters.items()},
            {key: dict(value) for key, value in self.pending.items()},
            dict(self.request_to_waiter),
            dict(self.waiter_to_request),
            {key: {waiter: dict(value) for waiter, value in waiters.items()} for key, value in self.condition_waiters.items()},
            dict(self.active_generation_by_process),
            [dict(item) for item in self.warnings],
            self.last_index,
            self.last_time,
            self.low_confidence,
            [dict(item) for item in self.cancelled_waiters],
        )
        return state
