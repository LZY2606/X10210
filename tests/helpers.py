from app.models import (
    Event, THREAD_START, THREAD_END, REQUEST, GRANT, RELEASE,
    COND_WAIT, COND_NOTIFY, COND_GRANT, CANCEL, EXCLUSIVE, SHARED,
)


_id = 0


def ev(kind, ts, thread, lock=None, gen=1, mode=EXCLUSIVE, cond=None,
       timeout=False, stack=None, **data):
    global _id
    _id += 1
    return Event(kind=kind, ts=ts, thread=thread, lock=lock, gen=gen,
                 mode=mode, cond=cond, timeout=timeout, stack=stack,
                 data=data, id=_id)


def reset_ids():
    global _id
    _id = 0


def starts(threads, gen=1, t0=0):
    return [ev(THREAD_START, t0 + i, t, gen=gen) for i, t in enumerate(threads)]


def run(events, lock_types=None):
    from app.engine import ReplayEngine
    return ReplayEngine(lock_types=lock_types).run(events)


def cycle_sigs(result, gen=1):
    return {c["signature"]
            for fr in result.cycles_at.values() for c in fr
            if c["gen"] == gen}
