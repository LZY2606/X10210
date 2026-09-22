from lab.minimize import minimize


def _abba_with_noise():
    events = []
    ts = 0

    def add(t, th, l=None, **kw):
        nonlocal ts
        ts += 1
        events.append({"type": t, "thread": th, "lock": l, "ts": ts, **kw})

    # 无关噪声：另一个线程独立获取释放
    add("thread_start", "NOISE")
    add("request", "NOISE", "N1")
    add("acquired", "NOISE", "N1")
    add("release", "NOISE", "N1")
    # 环
    add("thread_start", "A")
    add("thread_start", "B")
    add("request", "A", "La")
    add("acquired", "A", "La")
    add("request", "B", "Lb")
    add("acquired", "B", "Lb")
    add("request", "A", "Lb")
    add("request", "B", "La")
    return events


def test_minimizer_keeps_pairing_and_reproduces(make_timeline):
    tl = make_timeline(_abba_with_noise())
    result = minimize(tl)
    assert result["found"] and result["reproduced"]
    assert result["reduced_count"] < result["original_count"]
    threads = {e["thread"] for e in result["events"]}
    assert threads == {"A", "B"}  # 噪声线程整体消失
    types = [e["type"] for e in result["events"]]
    # request 与 acquired 配对保留
    assert types.count("request") == 4
    assert types.count("acquired") == 2


def test_minimizer_preserves_condition_pairing(make_timeline):
    # condition wait 必须与 notify 成对保留才不会破坏语义
    tl = make_timeline([
        {"type": "request", "thread": "C", "lock": "M", "cond": "cv", "ts": 1},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 2},
        {"type": "cond_wait", "thread": "C", "lock": "M", "cond": "cv", "ts": 3},
        {"type": "request", "thread": "S", "lock": "M", "ts": 4},
        {"type": "acquired", "thread": "S", "lock": "M", "ts": 5},
        {"type": "cond_notify", "thread": "S", "lock": "M", "cond": "cv", "ts": 6},
        {"type": "release", "thread": "S", "lock": "M", "ts": 7},
        {"type": "acquired", "thread": "C", "lock": "M", "ts": 8},
    ])
    # 无环 -> 明确返回 found=False 而不是报错
    result = minimize(tl)
    assert result["found"] is False
