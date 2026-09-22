from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .model import (
    ACQUIRE_REQUEST, ACQUIRE_SUCCESS, CANCEL, CONDITION_NOTIFY, CONDITION_WAIT,
    EXCLUSIVE, LOCK_TYPE, MUTEX, PROCESS_EXIT, PROCESS_START, RELEASE,
    RECURSIVE_MUTEX, RWLOCK, SHARED, THREAD_END, THREAD_START, TIMEOUT,
    UPGRADE, AnalysisState, LockHeld, LockState, ThreadState,
)

EVENT_TYPES = {
    ACQUIRE_REQUEST, ACQUIRE_SUCCESS, RELEASE, THREAD_START, THREAD_END,
    CONDITION_WAIT, CONDITION_NOTIFY, TIMEOUT, CANCEL, PROCESS_START,
    PROCESS_EXIT, LOCK_TYPE,
}
REQUEST_EVENTS = {ACQUIRE_REQUEST, TIMEOUT, CANCEL}


def sorted_events(events):
    return sorted(events, key=lambda e: (
        float(e.get('timestamp', 0) or 0),
        int(e.get('seq', e.get('arrival_seq', 0)) or 0),
        str(e.get('id', '')),
    ))


def thread_id(event):
    return '%s/%s' % (event.get('process_id', 'p0'), event.get('thread_id') or event.get('thread') or 't?')


def lock_address(event):
    return str(event.get('lock_id') or event.get('address') or event.get('lock') or '?')


def lock_id(event, generation):
    return '%s:%s' % (generation, lock_address(event))


def request_id(event, fallback):
    return str(event.get('request_id') or event.get('id') or fallback)


def waiter_id(event, fallback):
    return str(event.get('waiter_id') or event.get('request_id') or event.get('id') or fallback)


def waiter_request_id(event, fallback):
    return str(event.get('request_id') or event.get('waiter_id') or event.get('id') or fallback)


def mode_of(event):
    mode = str(event.get('mode') or EXCLUSIVE).lower()
    return mode if mode in (SHARED, UPGRADE) else EXCLUSIVE


def stack_of(event):
    stack = event.get('stack') or event.get('stacktrace') or []
    if isinstance(stack, str):
        return [{'symbol': line} for line in stack.splitlines() if line]
    return [dict(frame) if isinstance(frame, dict) else {'symbol': str(frame)} for frame in stack]


def confidence_for(low=False, disputed=False):
    return 'low' if low or disputed else 'high'


def generation_for_event(event, state):
    process = str(event.get('process_id', 'p0'))
    explicit = event.get('generation') or event.get('gen')
    if explicit:
        return str(explicit)
    return state.active_generation_by_process.get(process, '%s:g0' % process)


def warn(state, event, code, message, severity='warning'):
    state.warnings.append({'event_id': event.get('id'), 'code': code, 'message': message,
                           'severity': severity, 'timestamp': event.get('timestamp')})
    if severity == 'warning':
        state.low_confidence = True


def ensure_thread(state, event):
    tid = thread_id(event)
    if tid not in state.threads:
        state.threads[tid] = ThreadState(tid)
    return state.threads[tid]


def ensure_lock(state, event, generation=None):
    gen = generation or generation_for_event(event, state)
    key = lock_id(event, gen)
    if key not in state.locks:
        state.locks[key] = LockState(key, key, lock_address(event), gen)
        state.lock_waiters[key] = {}
    return state.locks[key]


def held_index(state, tid, key):
    for index, item in enumerate(state.held.get(tid, [])):
        if item.key == key:
            return index
    return None


def current_mode(item):
    return EXCLUSIVE if item.mode == UPGRADE or item.count < 0 else item.mode


def waiter_maps(waiter):
    shared = waiter.get('shared_threads') if isinstance(waiter.get('shared_threads'), dict) else {}
    return int(waiter.get('exclusive_holders', 0) or 0), sum(int(v) for v in shared.values()), list(shared.keys())


def _holder_threads(lock):
    holders = []
    if lock.exclusive_owner:
        holders.append((lock.exclusive_owner, EXCLUSIVE, 'exclusive-owner'))
    for tid in sorted(lock.mode.shared):
        if lock.mode.shared[tid] > 0:
            holders.append((tid, SHARED, 'shared-holder'))
    return holders


def _queue_before(lock, waiter):
    result = []
    seq = waiter.get('queue_seq', 0)
    for wid, other in lock.waiter_events.items():
        if wid != waiter.get('id') and int(other.get('queue_seq', 0)) < seq:
            result.append((other.get('thread_id'), other.get('mode'), wid))
    return result


def _compatible(holder_mode, waiter_mode):
    return holder_mode == SHARED and waiter_mode == SHARED


def blockers_for(lock, waiter, state=None):
    tid = waiter['thread_id']
    blockers = []
    for holder, holder_mode, reason in _holder_threads(lock):
        if holder == tid and waiter_mode_allows_self(waiter, holder_mode, state, lock):
            continue
        if _compatible(holder_mode, waiter.get('mode', EXCLUSIVE)):
            continue
        blockers.append({'thread_id': holder, 'reason': reason, 'mode': holder_mode,
                             'hold_event_id': holder_event(lock, holder, state), 'kind': 'holder'})
    for predecessor, predecessor_mode, wid in _queue_before(lock, waiter):
        if predecessor and predecessor != tid and not _compatible(predecessor_mode, waiter.get('mode', EXCLUSIVE)):
            blockers.append({'thread_id': predecessor, 'reason': 'queue-predecessor',
                             'mode': predecessor_mode, 'waiter_id': wid, 'kind': 'fairness'})
    return blockers


def waiter_mode_allows_self(waiter, holder_mode, state=None, lock=None):
    if waiter.get('mode') == UPGRADE and state is not None and lock is not None:
        existing = held_index(state, waiter['thread_id'], lock.key)
        return holder_mode == SHARED and existing is not None and waiter.get('upgrades_self', True)
    return True


def holder_event(lock, tid, state=None):
    if lock.exclusive_owner == tid:
        return lock.held_event
    if state is not None:
        index = held_index(state, tid, lock.key)
        if index is not None:
            return state.held[tid][index].event_id
    return None


def add_hold(state, tid, key, event, mode, confidence='high', status='held'):
    lock = state.locks[key]
    item = LockHeld(
        lock_id=key, key=key, address=lock.address, generation=lock.generation,
        mode=mode, count=1, stack=stack_of(event), acquire_event_id=str(event.get('id')),
        event_id=str(event.get('id')), confidence=confidence, status=status,
    )
    state.held.setdefault(tid, []).append(item)
    if mode == SHARED:
        lock.mode.shared[tid] = lock.mode.shared.get(tid, 0) + 1
    else:
        lock.mode.exclusive += 1
        lock.exclusive_owner = tid
        lock.held_event = str(event.get('id'))
    lock.event_id = str(event.get('id'))
    return item


def remove_waiter(state, key, wid):
    lock = state.locks.get(key)
    waiter = state.lock_waiters.get(key, {}).pop(wid, None)
    if lock and wid in lock.waiter_events:
        lock.waiter_events.pop(wid, None)
    if waiter:
        req = waiter.get('request_id')
        state.pending.pop(req or wid, None)
        state.request_to_waiter.pop(req or wid, None)
        state.waiter_to_request.pop(wid, None)
    return waiter


def make_waiter(event, state, key, wid, mode=None, kind='lock', source_event=None, extra=None):
    tid = thread_id(event)
    lock = state.locks[key]
    waiter = {
        'id': wid,
        'thread_id': tid,
        'lock_key': key,
        'mode': mode or mode_of(event),
        'request_id': request_id(event, wid),
        'event_id': str(event.get('id')),
        'request_event_id': str((source_event or event).get('id')),
        'stack': stack_of(source_event or event),
        'timestamp': event.get('timestamp'),
        'queue_seq': int(event.get('seq', event.get('arrival_seq', state.last_index + 1)) or state.last_index + 1),
        'kind': kind,
        'source': source_event.get('type') if source_event else event.get('type'),
        'source_event_id': str(source_event.get('id')) if source_event else str(event.get('id')),
    }
    if extra:
        waiter.update(extra)
    state.lock_waiters.setdefault(key, {})[wid] = waiter
    lock.waiter_events[wid] = waiter
    state.pending[waiter['request_id']] = waiter
    state.request_to_waiter[waiter['request_id']] = wid
    state.waiter_to_request[wid] = waiter['request_id']
    return waiter


def grant_waiter(state, key, wid, event, force=False):
    waiter = state.lock_waiters.get(key, {}).get(wid)
    if not waiter:
        return None
    lock = state.locks[key]
    blockers = blockers_for(lock, waiter, state)
    hard_blockers = [b for b in blockers if b['kind'] == 'holder']
    if hard_blockers and not force:
        warn(state, event, 'premature_success',
             'Acquire success while lock is still held by another thread; retaining candidate holder.', 'warning')
        waiter['confidence'] = 'low'
        for released in waiter.get('released_holds', []):
            pass
        item = add_hold(state, waiter['thread_id'], key, event, waiter.get('mode', EXCLUSIVE), 'low', 'disputed')
        lock.conflicts.append(str(event.get('id')))
        lock.low_confidence = True
        remove_waiter(state, key, wid)
        return item
    return _commit_grant(state, key, wid, event)


def _commit_grant(state, key, wid, event):
    waiter = remove_waiter(state, key, wid)
    lock = state.locks[key]
    tid = waiter['thread_id']
    mode = waiter.get('mode', EXCLUSIVE)
    released = waiter.get('released_holds') or []
    if mode == UPGRADE:
        index = held_index(state, tid, key)
        if index is not None:
            old = state.held[tid][index]
            lock.mode.shared[tid] = max(0, lock.mode.shared.get(tid, 0) - old.count)
            old.mode = UPGRADE
            old.count = 1
            old.stack = stack_of(event) or old.stack
            old.acquire_event_id = str(event.get('id'))
            old.event_id = str(event.get('id'))
        else:
            add_hold(state, tid, key, event, EXCLUSIVE)
        lock.mode.exclusive += 1
        lock.exclusive_owner = tid
        lock.held_event = str(event.get('id'))
        return state.held[tid][held_index(state, tid, key)]
    if released:
        restored = []
        for saved in released:
            restored.append(add_hold(state, tid, key, event, saved.get('mode', EXCLUSIVE), saved.get('confidence', 'high'), saved.get('status', 'held')))
        return restored[0] if restored else None
    return add_hold(state, tid, key, event, mode)


def _pop_hold_for_release(state, tid, key, event):
    idx = held_index(state, tid, key)
    if idx is None:
        return None
    item = state.held[tid].pop(idx)
    lock = state.locks[key]
    mode = current_mode(item)
    if mode == SHARED:
        lock.mode.shared[tid] = max(0, lock.mode.shared.get(tid, 0) - item.count)
    else:
        lock.mode.exclusive = max(0, lock.mode.exclusive - item.count)
        if lock.exclusive_owner == tid:
            lock.exclusive_owner = None
            lock.held_event = None
    item.release_event_id = str(event.get('id'))
    item.status = 'released'
    return item


def apply_acquire_request(state, event):
    tid = thread_id(event)
    thread = ensure_thread(state, event)
    if thread.ended:
        warn(state, event, 'request_after_thread_end', 'Acquire request after thread end; retaining as low-confidence candidate.')
    gen = generation_for_event(event, state)
    lock = ensure_lock(event=event, state=state, generation=gen)
    key = lock.key
    mode = mode_of(event)
    wid = waiter_id(event, '%s:%s' % (key, event.get('id')))
    if wid in state.lock_waiters.get(key, {}):
        warn(state, event, 'duplicate_waiter', 'Duplicate waiter id ignored.')
        return
    existing = held_index(state, tid, key)
    extra = {}
    if existing is not None and lock.lock_type == RECURSIVE_MUTEX:
        # A recursive request may be satisfied without an observed success.
        item = state.held[tid][existing]
        item.count += 1
        item.stack = stack_of(event) or item.stack
        item.event_id = str(event.get('id'))
        if item.mode != SHARED:
            lock.mode.exclusive += 1
        else:
            lock.mode.shared[tid] = lock.mode.shared.get(tid, 0) + 1
        return
    if existing is not None and lock.lock_type == RWLOCK:
        old = state.held[tid][existing]
        if old.mode == SHARED and mode == UPGRADE:
            extra['upgrades_self'] = True
            extra['upgraded_count'] = old.count
        elif mode == SHARED:
            old.count += 1
            lock.mode.shared[tid] = lock.mode.shared.get(tid, 0) + 1
            old.event_id = str(event.get('id'))
            return
        elif current_mode(old) == EXCLUSIVE:
            old.count += 1
            lock.mode.exclusive += 1
            old.event_id = str(event.get('id'))
            return
        else:
            warn(state, event, 'rw_reacquire_conflict', 'A read-write lock re-acquire is not legal for current mode.')
    make_waiter(event, state, key, wid, mode=mode, kind='lock', extra=extra)


def apply_acquire_success(state, event):
    tid = thread_id(event)
    ensure_thread(state, event)
    gen = generation_for_event(event, state)
    lock = ensure_lock(event=event, state=state, generation=gen)
    key = lock.key
    rid = waiter_request_id(event, '%s:%s' % (key, event.get('id')))
    wid = state.request_to_waiter.get(rid) or state.request_to_waiter.get(str(event.get('id')))
    if not wid:
        matching = [w for w in state.lock_waiters.get(key, {}).values() if w['thread_id'] == tid]
        wid = matching[0]['id'] if matching else None
    if wid:
        grant_waiter(state, key, wid, event)
        return
    mode = mode_of(event)
    existing = held_index(state, tid, key)
    if existing is not None:
        item = state.held[tid][existing]
        if lock.lock_type == RECURSIVE_MUTEX or current_mode(item) == EXCLUSIVE:
            item.count += 1
            lock.mode.exclusive += 1
            item.event_id = str(event.get('id'))
            return
        if mode == SHARED and item.mode == SHARED:
            item.count += 1
            lock.mode.shared[tid] = lock.mode.shared.get(tid, 0) + 1
            item.event_id = str(event.get('id'))
            return
    if lock.mode.total() and lock.exclusive_owner != tid and not (mode == SHARED and lock.mode.exclusive == 0):
        warn(state, event, 'success_without_release',
             'Acquire success has no matching request and lock appears held; candidate graph is low confidence.')
        add_hold(state, tid, key, event, mode, confidence='low', status='disputed')
        lock.conflicts.append(str(event.get('id')))
        lock.low_confidence = True
        return
    add_hold(state, tid, key, event, mode)


def apply_release(state, event):
    tid = thread_id(event)
    ensure_thread(state, event)
    gen = generation_for_event(event, state)
    lock = ensure_lock(event=event, state=state, generation=gen)
    item = _pop_hold_for_release(state, tid, lock.key, event)
    if item is None:
        warn(state, event, 'unmatched_release', 'Release without an observed acquire; ignored rather than inventing ownership.')
        return
    if item.mode == UPGRADE and event.get('mode') == SHARED:
        # Some traces represent an upgrade downgrade as release(S)/reacquire(S).
        add_hold(state, tid, lock.key, event, SHARED, confidence=item.confidence, status=item.status)


def _all_holds(state, tid, key):
    return [item for item in state.held.get(tid, []) if item.key == key]


def _release_all_for_condition(state, tid, key, event):
    saved = []
    lock = state.locks[key]
    for item in list(state.held.get(tid, [])):
        if item.key != key:
            continue
        saved.append({
            'mode': item.mode, 'count': item.count, 'stack': [dict(f) for f in item.stack],
            'acquire_event_id': item.acquire_event_id, 'event_id': item.event_id,
            'confidence': item.confidence, 'status': item.status,
        })
    # Remove as a group while retaining saved counts for semantic reacquisition.
    state.held[tid] = [item for item in state.held.get(tid, []) if item.key != key]
    lock.mode.shared[tid] = 0
    if lock.exclusive_owner == tid:
        lock.mode.exclusive = 0
        lock.exclusive_owner = None
        lock.held_event = None
    return saved


def apply_condition_wait(state, event):
    tid = thread_id(event)
    ensure_thread(state, event)
    gen = generation_for_event(event, state)
    lock = ensure_lock(event=event, state=state, generation=gen)
    key = lock.key
    if held_index(state, tid, key) is None:
        warn(state, event, 'condition_wait_without_hold', 'Condition wait without ownership of the associated lock.')
        return
    cond = str(event.get('condition_id') or event.get('condition') or lock.address + ':cond')
    saved = _release_all_for_condition(state, tid, key, event)
    wid = waiter_id(event, '%s:wait:%s' % (cond, event.get('id')))
    waiter = {
        'id': wid,
        'thread_id': tid,
        'lock_key': key,
        'condition_id': cond,
        'mode': saved[0]['mode'] if saved else EXCLUSIVE,
        'request_id': request_id(event, wid),
        'event_id': str(event.get('id')),
        'request_event_id': str(event.get('id')),
        'stack': stack_of(event),
        'timestamp': event.get('timestamp'),
        'queue_seq': int(event.get('seq', event.get('arrival_seq', state.last_index + 1)) or state.last_index + 1),
        'kind': 'condition_wait',
        'source': CONDITION_WAIT,
        'source_event_id': str(event.get('id')),
        'released_holds': saved,
        'confidence': 'high',
    }
    state.condition_waiters.setdefault(cond, {})[wid] = waiter


def _remove_condition_waiter(state, wid):
    for cond, waiters in list(state.condition_waiters.items()):
        if wid in waiters:
            return waiters.pop(wid)
        for candidate in list(waiters.values()):
            if candidate.get('request_id') == wid or candidate.get('id') == wid:
                return waiters.pop(candidate['id'])
    return None


def apply_condition_notify(state, event):
    ensure_thread(state, event)
    cond = str(event.get('condition_id') or event.get('condition') or lock_address(event) + ':cond')
    target = event.get('thread_id')
    all_waiters = list(state.condition_waiters.get(cond, {}).values())
    chosen = None
    if target:
        wanted = thread_id(event)
        chosen = next((w for w in all_waiters if w['thread_id'] == wanted), None)
    if chosen is None and all_waiters:
        chosen = all_waiters[0]
    if chosen is None:
        warn(state, event, 'notify_without_waiter', 'Condition notify did not match a waiting thread.', 'info')
        return
    state.condition_waiters[cond].pop(chosen['id'])
    wid = '%s:reacquire:%s' % (chosen['id'], event.get('id'))
    if wid in state.lock_waiters.get(chosen['lock_key'], {}):
        return
    synthetic_event = dict(event)
    synthetic_event['thread_id'] = chosen['thread_id'].split('/', 1)[-1]
    synthetic_event['process_id'] = chosen['thread_id'].split('/', 1)[0]
    synthetic_event['mode'] = chosen['mode']
    synthetic_event['stack'] = chosen['stack']
    synthetic_event['request_id'] = chosen['request_id']
    synthetic_event['waiter_id'] = wid
    make_waiter(synthetic_event, state, chosen['lock_key'], wid, mode=chosen['mode'],
                kind='condition_reacquire', source_event=event,
                extra={'released_holds': chosen['released_holds'], 'condition_id': cond,
                       'source_event_id': chosen['source_event_id'], 'confidence': 'high'})


def apply_cancel_or_timeout(state, event):
    rid = waiter_request_id(event, str(event.get('id')))
    wid = state.request_to_waiter.get(rid)
    if wid:
        for key, waiters in state.lock_waiters.items():
            if wid in waiters:
                waiter = remove_waiter(state, key, wid)
                waiter['cancel_event_id'] = str(event.get('id'))
                waiter['cancel_reason'] = event['type']
                state.cancelled_waiters.append(waiter)
                return
    condition_waiter = _remove_condition_waiter(state, rid)
    if condition_waiter is None:
        warn(state, event, 'cancel_without_waiter', 'Timeout or cancel did not match a pending request.', 'info')
    else:
        condition_waiter['cancel_event_id'] = str(event.get('id'))
        condition_waiter['cancel_reason'] = event['type']
        state.cancelled_waiters.append(condition_waiter)


def apply_thread_end(state, event):
    tid = thread_id(event)
    thread = ensure_thread(state, event)
    thread.ended = True
    thread.end_event = str(event.get('id'))
    for key, waiters in list(state.lock_waiters.items()):
        for wid, waiter in list(waiters.items()):
            if waiter['thread_id'] == tid:
                cancelled = remove_waiter(state, key, wid)
                cancelled['cancel_reason'] = THREAD_END
                cancelled['cancel_event_id'] = str(event.get('id'))
                state.cancelled_waiters.append(cancelled)
    for cond, waiters in list(state.condition_waiters.items()):
        for wid, waiter in list(waiters.items()):
            if waiter['thread_id'] == tid:
                cancelled = waiters.pop(wid)
                cancelled['cancel_reason'] = THREAD_END
                cancelled['cancel_event_id'] = str(event.get('id'))
                state.cancelled_waiters.append(cancelled)
    for item in state.held.get(tid, []):
        item.status = 'abandoned'
        item.thread_end_event_id = str(event.get('id'))
        state.locks[item.key].low_confidence = True
    state.low_confidence = True


def apply_thread_start(state, event):
    thread = ensure_thread(state, event)
    thread.started = True
    thread.start_event = str(event.get('id'))
    if thread.ended:
        warn(state, event, 'thread_id_reused', 'Thread id observed after an end event; treated as a new candidate lifecycle.')
        thread.ended = False
        thread.end_event = None


def apply_lock_type(state, event):
    gen = generation_for_event(event, state)
    lock = ensure_lock(event=event, state=state, generation=gen)
    lock_type = str(event.get('lock_type') or event.get('type_value') or MUTEX).lower()
    if lock_type not in (MUTEX, RECURSIVE_MUTEX, RWLOCK):
        warn(state, event, 'unknown_lock_type', 'Unknown lock type declaration ignored.')
        return
    lock.lock_type = lock_type
    lock.declared_type = True
    lock.type_event_id = str(event.get('id'))


def apply_process_boundary(state, event):
    process = str(event.get('process_id', 'p0'))
    if event['type'] == PROCESS_START:
        gen = str(event.get('generation') or event.get('gen') or '%s:g%s' % (process, event.get('timestamp', state.last_index + 1)))
        state.active_generation_by_process[process] = gen
    else:
        state.active_generation_by_process.pop(process, None)


def apply_event(state, event):
    kind = event.get('type')
    if kind not in EVENT_TYPES:
        warn(state, event, 'unknown_event', 'Unknown event type %s' % kind, 'info')
        return state
    ensure_thread(state, event)
    if kind == ACQUIRE_REQUEST:
        apply_acquire_request(state, event)
    elif kind == ACQUIRE_SUCCESS:
        apply_acquire_success(state, event)
    elif kind == RELEASE:
        apply_release(state, event)
    elif kind == THREAD_START:
        apply_thread_start(state, event)
    elif kind == THREAD_END:
        apply_thread_end(state, event)
    elif kind == CONDITION_WAIT:
        apply_condition_wait(state, event)
    elif kind == CONDITION_NOTIFY:
        apply_condition_notify(state, event)
    elif kind in (TIMEOUT, CANCEL):
        apply_cancel_or_timeout(state, event)
    elif kind == LOCK_TYPE:
        apply_lock_type(state, event)
    elif kind in (PROCESS_START, PROCESS_EXIT):
        apply_process_boundary(state, event)
    state.last_index = int(event.get('order_index', state.last_index + 1))
    state.last_time = event.get('timestamp')
    return state


def initial_state():
    return AnalysisState()


def analyze_events(events, state=None, start_index=0, end_index=None):
    ordered = sorted_events(events)
    state = state.clone() if state is not None else initial_state()
    stop = len(ordered) if end_index is None else min(end_index + 1, len(ordered))
    for index in range(max(0, start_index), stop):
        event = dict(ordered[index])
        event['order_index'] = index
        apply_event(state, event)
    ordered = enrich_ordered(ordered)
    return state, ordered


def enrich_ordered(ordered):
    return [dict(event, order_index=index) for index, event in enumerate(ordered)]


def hold_view(item):
    return {
        'thread_id': None,
        'lock_key': item.key,
        'address': item.address,
        'generation': item.generation,
        'mode': item.mode,
        'count': item.count,
        'stack': item.stack,
        'acquire_event_id': item.acquire_event_id,
        'event_id': item.event_id,
        'release_event_id': getattr(item, 'release_event_id', None),
        'confidence': item.confidence,
        'status': item.status,
    }


def build_views(state):
    locks = []
    for key, lock in sorted(state.locks.items()):
        locks.append({
            'id': key,
            'address': lock.address,
            'generation': lock.generation,
            'type': lock.lock_type,
            'declared_type': lock.declared_type,
            'exclusive_owner': lock.exclusive_owner,
            'shared_owners': {tid: count for tid, count in lock.mode.shared.items() if count},
            'waiter_count': len(lock.waiter_events),
            'confidence': 'low' if lock.low_confidence or lock.conflicts else 'high',
            'conflicts': list(lock.conflicts),
        })
    threads = []
    held_edges = []
    for tid, stack in sorted(state.held.items()):
        for depth, item in enumerate(stack):
            view = hold_view(item)
            view['thread_id'] = tid
            view['depth'] = depth
            held_edges.append(view)
        info = state.threads.get(tid)
        threads.append({
            'id': tid,
            'started': bool(info and info.started),
            'ended': bool(info and info.ended),
            'start_event_id': info.start_event if info else None,
            'end_event_id': info.end_event if info else None,
            'holds': len(stack),
            'confidence': 'low' if any(item.status == 'abandoned' for item in stack) else 'high',
        })
    wait_edges = []
    condition_edges = []
    for key, waiters in sorted(state.lock_waiters.items()):
        lock = state.locks[key]
        for wid, waiter in sorted(waiters.items(), key=lambda pair: pair[1].get('queue_seq', 0)):
            blockers = blockers_for(lock, waiter, state)
            edge = {
                'id': wid,
                'thread_id': waiter['thread_id'],
                'lock_key': key,
                'address': lock.address,
                'generation': lock.generation,
                'mode': waiter.get('mode'),
                'kind': waiter.get('kind', 'lock'),
                'request_event_id': waiter.get('request_event_id'),
                'event_id': waiter.get('event_id'),
                'source_event_id': waiter.get('source_event_id'),
                'stack': waiter.get('stack', []),
                'timestamp': waiter.get('timestamp'),
                'blockers': blockers,
                'confidence': waiter.get('confidence', 'high'),
            }
            if waiter.get('condition_id'):
                edge['condition_id'] = waiter['condition_id']
            wait_edges.append(edge)
    for cond, waiters in sorted(state.condition_waiters.items()):
        for wid, waiter in waiters.items():
            condition_edges.append({
                'id': wid,
                'condition_id': cond,
                'thread_id': waiter['thread_id'],
                'lock_key': waiter['lock_key'],
                'request_event_id': waiter.get('request_event_id'),
                'event_id': waiter.get('event_id'),
                'stack': waiter.get('stack', []),
                'timestamp': waiter.get('timestamp'),
                'state': 'waiting',
                'confidence': 'high',
            })
    cycles = find_cycles(wait_edges, state)
    return {
        'threads': threads,
        'locks': locks,
        'held_graph': {'nodes': graph_nodes(held_edges, wait_edges, condition_edges), 'edges': held_edges},
        'wait_graph': {'nodes': graph_nodes(held_edges, wait_edges, condition_edges), 'edges': wait_edges},
        'condition_waits': condition_edges,
        'cancelled_waiters': state.cancelled_waiters,
        'cycles': cycles,
        'warnings': state.warnings,
        'confidence': 'low' if state.low_confidence else 'high',
    }


def graph_nodes(held_edges, wait_edges, condition_edges):
    threads = set()
    locks = set()
    conditions = set()
    for item in held_edges:
        threads.add(item['thread_id'])
        locks.add(item['lock_key'])
    for item in wait_edges:
        threads.add(item['thread_id'])
        locks.add(item['lock_key'])
    for item in condition_edges:
        threads.add(item['thread_id'])
        locks.add(item['lock_key'])
        conditions.add(item['condition_id'])
    return [{'id': item, 'kind': 'thread'} for item in sorted(threads)] + \
           [{'id': item, 'kind': 'lock'} for item in sorted(locks)] + \
           [{'id': item, 'kind': 'condition'} for item in sorted(conditions)]


def _hold_lookup(state):
    lookup = {}
    for tid, holds in state.held.items():
        for item in holds:
            lookup[(tid, item.key)] = item
    return lookup


def _waiter_adjacency(wait_edges, state):
    hold_lookup = _hold_lookup(state)
    adjacency = defaultdict(list)
    for edge in wait_edges:
        waiter_tid = edge['thread_id']
        seen = set()
        for blocker in edge['blockers']:
            if blocker.get('kind') != 'holder':
                continue
            blocked_tid = blocker.get('thread_id')
            if not blocked_tid or blocked_tid == waiter_tid or blocked_tid in seen:
                continue
            seen.add(blocked_tid)
            held = hold_lookup.get((blocked_tid, edge['lock_key']))
            adjacency[waiter_tid].append({
                'from_thread': waiter_tid,
                'to_thread': blocked_tid,
                'lock_key': edge['lock_key'],
                'address': edge.get('address'),
                'generation': edge.get('generation'),
                'wait': {
                    'event_id': edge.get('event_id'),
                    'request_event_id': edge.get('request_event_id'),
                    'mode': edge.get('mode'),
                    'stack': edge.get('stack', []),
                    'timestamp': edge.get('timestamp'),
                    'confidence': edge.get('confidence', 'high'),
                },
                'hold': {
                    'event_id': getattr(held, 'event_id', None) if held else blocker.get('hold_event_id'),
                    'acquire_event_id': getattr(held, 'acquire_event_id', None) if held else None,
                    'mode': getattr(held, 'mode', blocker.get('mode')) if held else blocker.get('mode'),
                    'count': getattr(held, 'count', 1) if held else 1,
                    'stack': getattr(held, 'stack', []) if held else [],
                    'confidence': getattr(held, 'confidence', 'high') if held else 'high',
                },
            })
    return adjacency


def find_cycles(wait_edges, state):
    adjacency = _waiter_adjacency(wait_edges, state)
    cycles = []
    seen_signatures = set()

    def dfs(start, current, path, visited):
        for edge in adjacency.get(current, []):
            nxt = edge['to_thread']
            if nxt == start and path:
                cycle_edges = path + [edge]
                signature = canonical_cycle(cycle_edges)
                if signature not in seen_signatures:
                    seen_signatures.add(signature)
                    cycles.append(cycle_report(cycle_edges))
            elif nxt not in visited and nxt > start:
                visited.add(nxt)
                dfs(start, nxt, path + [edge], visited)
                visited.discard(nxt)

    for start in sorted(adjacency.keys()):
        dfs(start, start, [], {start})
    cycles.sort(key=lambda report: ([item['thread_id'] for item in report['threads']], report['id']))
    for index, report in enumerate(cycles):
        report['id'] = 'cycle-%d' % (index + 1)
    return cycles


def canonical_cycle(edges):
    variants = []
    for offset in range(len(edges)):
        rotated = edges[offset:] + edges[:offset]
        variants.append(tuple((edge['from_thread'], edge['lock_key']) for edge in rotated))
    return min(variants)


def cycle_report(edges):
    low = any(edge['wait'].get('confidence') == 'low' or edge['hold'].get('confidence') == 'low' for edge in edges)
    return {
        'id': 'cycle',
        'threads': [{'thread_id': edge['from_thread'], 'wait_stack': edge['wait']['stack']} for edge in edges],
        'locks': [{'lock_key': edge['lock_key'], 'address': edge.get('address'), 'generation': edge.get('generation')} for edge in edges],
        'edges': edges,
        'confidence': 'low' if low else 'high',
        'summary': ' -> '.join([edge['from_thread'] + ' waits for ' + edge['lock_key'] + ' held by ' + edge['to_thread'] for edge in edges]),
    }


def timeline_points(ordered_events):
    return [
        {'index': index, 'timestamp': event.get('timestamp'), 'event_id': event.get('id'), 'type': event.get('type')}
        for index, event in enumerate(ordered_events)
    ]


def analyze(events, point_index=None):
    ordered = sorted_events(events)
    end = len(ordered) - 1 if point_index is None else point_index
    state, ordered = analyze_events(ordered, start_index=0, end_index=end)
    result = build_views(state)
    result['timeline'] = timeline_points(ordered)
    result['point_index'] = end
    result['events'] = ordered
    return result
