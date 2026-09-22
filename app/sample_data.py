"""Built-in demo trace.

A three-thread story: worker-1 and worker-2 walk into a classic
AB / BA deadlock (via a condition wait on one side), while a noisy
background thread generates unrelated acquire/release traffic so the
minimizer visibly shrinks the trace.
"""

SAMPLE_LOCK_TYPES = {"g0:guard": "recursive", "g0:cv_lock": "mutex"}

SAMPLE_EVENTS = [
    {"seq": 0, "ts": 1.0, "type": "thread_start", "thread": "worker-1"},
    {"seq": 1, "ts": 1.1, "type": "thread_start", "thread": "worker-2"},
    {"seq": 2, "ts": 1.2, "type": "thread_start", "thread": "bg-cleaner"},

    # background noise that the minimizer should remove
    {"seq": 3, "ts": 2.0, "type": "acquire_req", "thread": "bg-cleaner",
     "lock": "cache", "stack": ["cleaner.sweep", "cache.lock"]},
    {"seq": 4, "ts": 2.1, "type": "acquired", "thread": "bg-cleaner",
     "lock": "cache", "stack": ["cleaner.sweep", "cache.lock"]},

    # worker-1 takes A
    {"seq": 5, "ts": 3.0, "type": "acquire_req", "thread": "worker-1",
     "lock": "mutex-A", "stack": ["jobs.dispatch", "lock_A"]},
    {"seq": 6, "ts": 3.1, "type": "acquired", "thread": "worker-1",
     "lock": "mutex-A", "stack": ["jobs.dispatch", "lock_A"]},

    {"seq": 7, "ts": 3.4, "type": "release", "thread": "bg-cleaner",
     "lock": "cache", "stack": ["cleaner.sweep", "cache.unlock"]},

    # worker-2 takes B
    {"seq": 8, "ts": 4.0, "type": "acquire_req", "thread": "worker-2",
     "lock": "mutex-B", "stack": ["ledger.commit", "lock_B"]},
    {"seq": 9, "ts": 4.1, "type": "acquired", "thread": "worker-2",
     "lock": "mutex-B", "stack": ["ledger.commit", "lock_B"]},

    # worker-1 wants B and blocks
    {"seq": 10, "ts": 5.0, "type": "acquire_req", "thread": "worker-1",
     "lock": "mutex-B", "stack": ["jobs.dispatch", "ledger.commit",
                                  "lock_B"]},

    # worker-2 wants A and blocks behind worker-1
    {"seq": 11, "ts": 6.0, "type": "acquire_req", "thread": "worker-2",
     "lock": "mutex-A", "stack": ["ledger.commit", "jobs.sync",
                                  "lock_A"]},

    {"seq": 12, "ts": 7.0, "type": "thread_end", "thread": "bg-cleaner"},
]
