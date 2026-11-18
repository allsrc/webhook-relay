"""PostgreSQL store tests. Set RELAY_TEST_PG_URL to a database you own (tables are dropped/truncated)."""
import os
import random
import threading
import time
from collections import defaultdict

import httpx
import pytest

from webhook_relay.config import Route
from webhook_relay.delivery import Deliverer

URL = os.environ.get("RELAY_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not URL, reason="RELAY_TEST_PG_URL not set")

if URL:
    pytest.importorskip("psycopg_pool")
    from webhook_relay.store_pg import PostgresStore

P = "whatsapp"
R = Route("prod", "http://p/h")


@pytest.fixture()
def store():
    s = PostgresStore(URL, lease_seconds=10)
    s._exec("TRUNCATE deliveries RESTART IDENTITY", ())
    yield s
    s.close()


def enq(s, body=b"x", sender="", now=100.0, route=R):
    return s.enqueue(P, route.name, route.url, route.mode, body, {"h": "v"}, now=now, sender=sender)


def test_lifecycle_and_semantics(store):
    a = enq(store, b"a")
    assert store.due_routes(100.0) == [(P, "prod", "primary")]
    [row] = store.claim(P, "prod", 100.0, 5)
    assert row["id"] == a and bytes(row["body"]) == b"a" and store.get(a)["status"] == "inflight"
    store.mark_retry(a, 1, 150.0, "HTTP 500")
    assert store.claim(P, "prod", 120.0, 5) == []  # backing off
    [row] = store.claim(P, "prod", 150.0, 5)
    assert row["attempts"] == 1
    store.mark_dead(a, 8, 151.0, "boom")
    assert store.stats() == {"dead": 1}
    assert store.requeue_dead("other", 160.0) == 0
    assert store.requeue_dead("prod", 160.0) == 1
    [row] = store.claim(P, "prod", 160.0, 5)
    assert row["attempts"] == 0 and bytes(row["body"]) == b"a"  # body kept until replayed
    store.mark_done(a, 170.0)
    assert store.get(a)["body"] is None
    store.purge(170.0 + 3600, 1800)
    assert store.stats() == {}


def test_dead_retention_can_exceed_done(store):
    d, x = enq(store), enq(store)
    store.mark_dead(d, 8, 100.0, "e")
    store.mark_done(x, 100.0)
    store.purge(100.0 + 7200, 3600, dead_retain_seconds=86400)
    assert store.stats() == {"dead": 1}


def test_lane_ordering_at_claim(store):
    n1, n2 = enq(store, sender="B"), enq(store, sender="B")
    other = enq(store, sender="C")
    got = {r["id"] for r in store.claim(P, "prod", 100.0, 10)}
    assert got == {n1, other}  # B's N+1 waits
    assert store.claim(P, "prod", 100.0, 10) == []
    store.mark_done(n1, 101.0)
    assert [r["id"] for r in store.claim(P, "prod", 101.0, 10)] == [n2]


def test_lease_recovery(store):
    a = enq(store, sender="B")
    b = enq(store, sender="B")
    [row] = store.claim(P, "prod", 100.0, 1)  # replica 1 claims (lease until 110) then dies
    assert row["id"] == a
    other = PostgresStore(URL, lease_seconds=10)  # replica 2
    try:
        assert other.recover_inflight(105.0) == 0  # live lease is respected
        assert other.claim(P, "prod", 105.0, 5) == []  # lane still blocked by inflight N
        assert other.recover_inflight(111.0) == 1  # expired -> pending
        assert [r["id"] for r in other.claim(P, "prod", 111.0, 5)] == [a]  # still N before N+1
        other.mark_done(a, 112.0)
        assert [r["id"] for r in other.claim(P, "prod", 112.0, 5)] == [b]
        # claim() itself also reclaims expired leases
        assert [r["id"] for r in other.claim(P, "prod", 200.0, 5)] == [b]
    finally:
        other.close()


def test_concurrent_schema_creation():
    s = PostgresStore(URL)
    s._exec("DROP TABLE deliveries", ())
    s.close()
    errors, stores = [], []

    def boot():
        try:
            stores.append(PostgresStore(URL))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    ts = [threading.Thread(target=boot) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors and len(stores) == 6
    [s.close() for s in stores]


def test_two_replicas_no_double_delivery_and_lane_order(store):
    senders, per = 12, 8
    order, seen = defaultdict(list), []
    lock = threading.Lock()

    def handler(req):
        sender, seq = req.content.decode().split(":")
        time.sleep(random.random() * 0.01)
        with lock:
            seen.append(req.content)
            order[sender].append(int(seq))
        return httpx.Response(200)

    replicas = []
    for _ in range(2):
        st = PostgresStore(URL, lease_seconds=30)
        client = httpx.Client(transport=httpx.MockTransport(handler))
        replicas.append((st, Deliverer(st, client, routes={(P, "prod"): R})))
    for i in range(per):  # interleave senders, arrival order is the lane order
        for s in range(senders):
            enq(store, f"s{s}:{i}".encode(), sender=f"s{s}", now=time.time())
    for _, d in replicas:
        d.start()
    try:
        end = time.time() + 30
        while time.time() < end and store.stats().get("done", 0) < senders * per:
            time.sleep(0.05)
        assert store.stats() == {"done": senders * per}
        assert len(seen) == len(set(seen)) == senders * per  # no double delivery
        for s in range(senders):
            assert order[f"s{s}"] == list(range(per))  # strictly in order
    finally:
        for st, d in replicas:
            d.stop()
        for st, _ in replicas:
            st.close()
