from collections import defaultdict

from .analyzer import analyze, sorted_events
from .model import (
    ACQUIRE_REQUEST, ACQUIRE_SUCCESS, CANCEL, CONDITION_NOTIFY, CONDITION_WAIT,
    LOCK_TYPE, PROCESS_EXIT, PROCESS_START, RELEASE, THREAD_END, THREAD_START, TIMEOUT,
)


def _id(event):
    return str(event.get('id'))


def _rid(event):
    return str(event.get('request_id') or event.get('waiter_id') or event.get('id'))


def _event_groups(events):
    ordered = sorted_events(events)
    groups = {}
    stacks = defaultdict(list)

    def assign(event, group=None):
        gid = group or ('g-' + _id(event))
        groups[_id(event)] = gid

    for event in ordered:
        kind = event.get('type')
        key = '%s:%s:%s' % (event.get('process_id', 'p0'), event.get('thread_id') or event.get('thread'),
                            event.get('lock_id') or event.get('address') or event.get('lock'))
        if kind == THREAD_START:
            assign(event, 'lifecycle:%s:%s' % (event.get('process_id', 'p0'), event.get('thread_id') or event.get('thread')))
        elif kind == THREAD_END:
            assign(event, 'lifecycle:%s:%s' % (event.get('process_id', 'p0'), event.get('thread_id') or event.get('thread')))
        elif kind in (PROCESS_START, PROCESS_EXIT):
            assign(event, 'process:%s' % event.get('process_id', 'p0'))
        elif kind == LOCK_TYPE:
            assign(event, 'type:%s:%s' % (event.get('generation') or 'gen', key.rsplit(':', 1)[-1]))
        elif kind == ACQUIRE_REQUEST:
            stacks[key].append([event])
            assign(event)
        elif kind == ACQUIRE_SUCCESS:
            bucket = _take_request_bucket(stacks, key, event)
            bucket.append(event)
            assign(event, groups[_id(bucket[0])])
        elif kind == RELEASE:
            bucket = _find_acquired_bucket(stacks, key, event)
            if bucket:
                bucket.append(event)
                assign(event, groups[_id(bucket[0])])
            else:
                assign(event)
        elif kind == CONDITION_WAIT:
            assign(event)
        elif kind == CONDITION_NOTIFY:
            waits = [item for item in ordered if item.get('type') == CONDITION_WAIT and str(item.get('id')) in groups]
            assign(event)
            gid = groups[_id(event)]
            for wait in _matching_waits(ordered, event):
                groups[_id(wait)] = gid
        elif kind in (TIMEOUT, CANCEL):
            matched = _matching_request(ordered, event)
            if matched:
                assign(event, groups[_id(matched)])
            else:
                assign(event)
        else:
            assign(event)

    # A condition wait must never be split from its reacquisition/success/timeout.
    by_rid = {_rid(event): event for event in ordered}
    for event in ordered:
        if event.get('type') in (ACQUIRE_SUCCESS, TIMEOUT, CANCEL):
            matched = by_rid.get(_rid(event))
            if matched and matched.get('type') == CONDITION_WAIT:
                groups[_id(event)] = groups[_id(matched)]
    return ordered, groups


def _take_request_bucket(stacks, key, event):
    buckets = stacks.get(key, [])
    for index, bucket in enumerate(list(buckets)):
        request = bucket[0]
        if _rid(request) == _rid(event):
            return buckets.pop(index)
    if buckets:
        return buckets.pop(0)
    return [event]


def _find_acquired_bucket(stacks, key, event):
    for bucket in reversed(stacks.get(key, [])):
        if any(item.get('type') == ACQUIRE_SUCCESS for item in bucket):
            return bucket
    return None


def _matching_waits(ordered, notify):
    target = '%s/%s' % (notify.get('process_id', 'p0'), notify.get('thread_id'))
    result = []
    for event in ordered:
        if event.get('type') != CONDITION_WAIT:
            continue
        if notify.get('condition_id') not in (None, '') and event.get('condition_id') != notify.get('condition_id'):
            continue
        waiter_target = '%s/%s' % (event.get('process_id', 'p0'), event.get('thread_id'))
        if not notify.get('thread_id') or waiter_target == target:
            result.append(event)
    return result[:1]


def _matching_request(ordered, event):
    rid = _rid(event)
    for candidate in ordered:
        if candidate.get('type') in (ACQUIRE_REQUEST, CONDITION_WAIT) and _rid(candidate) == rid:
            return candidate
    return None


def _cycle_event_ids(cycle):
    ids = set()
    for edge in cycle.get('edges', []):
        wait = edge.get('wait', {})
        hold = edge.get('hold', {})
        ids.update(value for value in (wait.get('event_id'), wait.get('request_event_id'),
                                       hold.get('event_id'), hold.get('acquire_event_id')) if value)
    return ids


def _base_groups(ordered, groups, cycle, events):
    ids = _cycle_event_ids(cycle)
    selected = set(groups.get(item) for item in ids if groups.get(item))
    cycle_threads = {item['thread_id'] for item in cycle.get('threads', [])}
    for event in ordered:
        tid = '%s/%s' % (event.get('process_id', 'p0'), event.get('thread_id') or event.get('thread'))
        if tid in cycle_threads and event.get('type') == THREAD_START:
            selected.add(groups[_id(event)])
        if event.get('type') in (PROCESS_START, PROCESS_EXIT, LOCK_TYPE):
            selected.add(groups[_id(event)])
    return selected


def minimize_events(events, target_cycle=None):
    ordered, groups = _event_groups(events)
    full = analyze(ordered)
    cycle = target_cycle or (full['cycles'][0] if full['cycles'] else None)
    if not cycle:
        return {'events': [], 'groups': {}, 'cycle': None, 'removed_event_count': len(ordered)}
    signature = cycle_signature(cycle)
    selected = _base_groups(ordered, groups, cycle, ordered)

    # Start with every group, then greedily remove whole pairing/condition units.
    all_groups = sorted(set(groups.values()))
    current = set(all_groups)
    for group in all_groups:
        if group in selected:
            continue
        candidate = current - {group}
        reduced = [event for event in ordered if groups[_id(event)] in candidate]
        trial = analyze(reduced)
        if any(cycle_signature(item) == signature for item in trial['cycles']):
            current = candidate

    # The closure may already be a smaller legal set than the greedy result.
    closure = selected
    reduced = [event for event in ordered if groups[_id(event)] in closure]
    trial = analyze(reduced)
    if any(cycle_signature(item) == signature for item in trial['cycles']):
        current = closure

    result_events = [event for event in ordered if groups[_id(event)] in current]
    final = analyze(result_events)
    same = next((item for item in final['cycles'] if cycle_signature(item) == signature), None)
    return {
        'events': result_events,
        'groups': {gid: [_id(event) for event in ordered if groups[_id(event)] == gid] for gid in current},
        'cycle': same,
        'removed_event_count': len(ordered) - len(result_events),
    }


def cycle_signature(cycle):
    edges = cycle.get('edges', [])
    variants = []
    for offset in range(len(edges)):
        rotated = edges[offset:] + edges[:offset]
        variants.append(tuple((edge.get('from_thread'), edge.get('lock_key'), edge.get('to_thread')) for edge in rotated))
    return min(variants) if variants else tuple()
