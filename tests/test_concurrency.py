import threading
import time
from collections import Counter

import httpx

from webhook_relay.config import Route
from webhook_relay.delivery import Deliverer
from webhook_relay.store import Store

P = "whatsapp"


def make(handler, routes, **kw):
    store = Store(":memory:")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    rmap = {(P, r.name): r for r in routes}
    return store, Deliverer(store, client, routes=rmap, **kw)


def enqueue(store, route: Route, body: bytes, sender="", now=None):
    return store.enqueue(P, route.name, route.url, route.mode, body, {}, now=now, sender=sender)


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return False


def test_hanging_mirror_does_not_delay_primary():
    release = threading.Event()
    primary = Route("prod", "http://p/h")
    mirror = Route("laptop", "http://m/h", "mirror")

    def handler(req):
        if req.url.host == "m":
            release.wait(10)  # a mirror that never answers
            return httpx.Response(200)
        return httpx.Response(200)

    store, d = make(handler, [primary, mirror])
    d.start()
    try:
        # Plenty of mirror work first, so a shared queue would be stuck behind it.
        for i in range(10):
            enqueue(store, mirror, f"m{i}".encode(), sender=f"u{i}")
        t0 = time.time()
        ids = [enqueue(store, primary, f"p{i}".encode(), sender=f"u{i}") for i in range(10)]
        d.wake()
        assert wait_for(lambda: all(store.get(i)["status"] == "done" for i in ids), 3)
        assert time.time() - t0 < 2.0
        assert store.stats().get("inflight", 0) == 2  # only the mirror's own two workers are stuck
    finally:
        release.set()
        d.stop()


def test_different_senders_deliver_concurrently():
    route = Route("prod", "http://p/h")
    barrier = threading.Barrier(2, timeout=3)  # both requests must be in flight at once

    def handler(req):
        barrier.wait()
        return httpx.Response(200)

    store, d = make(handler, [route])
    a, b = enqueue(store, route, b"a", sender="111"), enqueue(store, route, b"b", sender="222")
    assert d.run_once() == 2
    assert store.get(a)["status"] == "done" and store.get(b)["status"] == "done"


def test_same_sender_stays_ordered_and_failing_sender_does_not_block_others():
    route = Route("prod", "http://p/h")
    seen, active, peak, lock = [], 0, 0, threading.Lock()
    failed_once = set()

    def handler(req):
        nonlocal active, peak
        body = req.content.decode()
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.02)
        with lock:
            active -= 1
            if body == "a1" and body not in failed_once:
                failed_once.add(body)
                return httpx.Response(503)
            seen.append(body)
        return httpx.Response(200)

    store, d = make(handler, [route])
    t = time.time()
    for body, sender in [("a1", "A"), ("a2", "A"), ("a3", "A"), ("b1", "B"), ("b2", "B")]:
        enqueue(store, route, body.encode(), sender, now=t)
    d.run_once(now=t)
    # A's first delivery failed and is backing off: A's later events wait, B is unaffected.
    assert seen == ["b1", "b2"] or sorted(seen) == ["b1", "b2"]
    assert [x for x in seen if x.startswith("b")] == ["b1", "b2"]
    d.run_once(now=t + 5)
    assert [x for x in seen if x.startswith("a")] == ["a1", "a2", "a3"]
    assert store.stats() == {"done": 5}


def test_same_sender_never_in_flight_twice():
    route = Route("prod", "http://p/h", workers=8)
    active, peak, lock, order = 0, 0, threading.Lock(), []

    def handler(req):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            order.append(int(req.content))
        time.sleep(0.01)
        with lock:
            active -= 1
        return httpx.Response(200)

    store, d = make(handler, [route])
    for i in range(15):
        enqueue(store, route, str(i).encode(), sender="same")
    d.run_once()
    assert order == list(range(15)) and peak == 1


def test_circuit_breaker_opens_and_recovers():
    mirror = Route("laptop", "http://m/h", "mirror", breaker_failures=2, breaker_cooldown=60)
    up = False
    calls = []

    def handler(req):
        calls.append(req.content)
        if not up:
            raise httpx.ConnectError("down", request=req)
        return httpx.Response(200)

    store, d = make(handler, [mirror])
    t = 1000.0
    ids = [enqueue(store, mirror, f"e{i}".encode(), sender=f"u{i}", now=t) for i in range(4)]
    d.run_once(now=t)
    assert len(calls) == 2  # two connect failures open the circuit; no further attempts are made
    d.run_once(now=t + 3)  # circuit open: the rest is marked dead without any network call
    assert len(calls) == 2
    assert all(store.get(i)["status"] == "dead" for i in ids)
    assert {store.get(i)["last_error"] for i in ids if store.get(i)["attempts"] == 0} == {"circuit_open"}

    late = enqueue(store, mirror, b"late", sender="u9", now=t + 10)
    d.run_once(now=t + 10)
    assert store.get(late)["status"] == "dead" and len(calls) == 2  # still no network call

    up = True  # laptop is back; cooldown elapsed, so a single probe goes out
    first = enqueue(store, mirror, b"probe", sender="u1", now=t + 70)
    second = enqueue(store, mirror, b"next", sender="u2", now=t + 70)
    d.run_once(now=t + 70)
    assert len(calls) == 4 and calls[2:] == [b"probe", b"next"]
    assert store.get(first)["status"] == "done" and store.get(second)["status"] == "done"
    assert d.breaker((P, "laptop"), "mirror").state(t + 71) == "closed"


def test_circuit_probe_failure_reopens():
    mirror = Route("laptop", "http://m/h", "mirror", breaker_failures=1, breaker_cooldown=30)
    calls = []

    def handler(req):
        calls.append(1)
        raise httpx.ConnectTimeout("down", request=req)

    store, d = make(handler, [mirror])
    enqueue(store, mirror, b"a", now=0.0)
    d.run_once(now=0.0)
    assert len(calls) == 1
    enqueue(store, mirror, b"b", now=31.0)
    enqueue(store, mirror, b"c", sender="x", now=31.0)
    d.run_once(now=31.0)
    assert len(calls) == 2  # exactly one probe, then open again
    d.run_once(now=32.0)
    assert len(calls) == 2


def test_no_double_delivery_with_many_workers():
    route = Route("prod", "http://p/h", workers=8)
    mirror = Route("mir", "http://m/h", "mirror", workers=4)
    counts, lock = Counter(), threading.Lock()

    def handler(req):
        with lock:
            counts[(req.url.host, req.content)] += 1
        time.sleep(0.001)
        return httpx.Response(200)

    store, d = make(handler, [route, mirror])
    n = 200
    for i in range(n):
        for r in (route, mirror):
            enqueue(store, r, str(i).encode(), sender=f"u{i % 17}")
    d.start()
    try:
        assert wait_for(lambda: store.stats() == {"done": 2 * n}, 20)
    finally:
        d.stop()
    assert len(counts) == 2 * n and set(counts.values()) == {1}


def test_inflight_rows_recovered_after_restart(tmp_path):
    path = str(tmp_path / "relay.db")
    route = Route("prod", "http://p/h")
    s1 = Store(path)
    a = enqueue(s1, route, b"a", sender="A")
    b = enqueue(s1, route, b"b", sender="A")
    assert [r["id"] for r in s1.claim(P, "prod", time.time(), 8)] == [a]  # claimed, then the process "dies"
    assert s1.get(a)["status"] == "inflight"

    got = []
    s2 = Store(path)  # new process
    d = Deliverer(s2, httpx.Client(transport=httpx.MockTransport(lambda r: (got.append(r.content), httpx.Response(200))[1])))
    assert d.recover() == 1
    d.run_once()
    assert got == [b"a", b"b"]  # redelivered, in order
    assert s2.stats() == {"done": 2}


def test_slow_delivery_logs_lag_warning(caplog):
    route = Route("prod", "http://p/h")
    store, d = make(lambda r: httpx.Response(200), [route], lag_warn_seconds=30)
    i = enqueue(store, route, b"secret-body", now=100.0)
    with caplog.at_level("WARNING", logger="relay"):
        d.run_once(now=145.0)
    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith(f"slow delivery route=prod id={i} lag=45.0s") for m in msgs)
    assert not any("secret-body" in m for m in msgs)
