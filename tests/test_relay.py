import hashlib
import hmac
import json
import time

import httpx
from fastapi.testclient import TestClient

from webhook_relay.app import create_app
from webhook_relay.config import Config, Provider, Route
from webhook_relay.delivery import Deliverer
from webhook_relay.store import Store

SECRET = "s3cret"


def payload(pnid="111"):
    return json.dumps({"entry": [{"changes": [{"value": {"metadata": {"phone_number_id": pnid}, "messages": [{"id": "m1"}]}}]}]}).encode()


def sig(body):
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def setup(routes, handler):
    cfg = Config(db_path=":memory:", providers=(Provider("whatsapp", "/whatsapp", "vt", SECRET, tuple(routes)),))
    store = Store(":memory:")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    d = Deliverer(store, client)
    return TestClient(create_app(cfg, store, d)), store, d


def test_challenge_and_signature():
    c, _, _ = setup([], lambda r: httpx.Response(200))
    assert c.get("/whatsapp", params={"hub.mode": "subscribe", "hub.verify_token": "vt", "hub.challenge": "9"}).text == "9"
    assert c.get("/whatsapp", params={"hub.mode": "subscribe", "hub.verify_token": "x", "hub.challenge": "9"}).status_code == 403
    assert c.post("/whatsapp", content=payload(), headers={"x-hub-signature-256": "sha256=00"}).status_code == 401


def test_body_and_signature_forwarded_unchanged_to_primary_and_mirror():
    seen = []
    c, _, d = setup(
        [Route("laptop", "http://l/hook"), Route("server", "http://s/hook", "mirror")],
        lambda r: (seen.append((str(r.url), r.content, r.headers)), httpx.Response(200))[1],
    )
    body = payload()
    assert c.post("/whatsapp", content=body, headers={"x-hub-signature-256": sig(body)}).json() == {"queued": 2}
    d.run_once()
    assert {u for u, _, _ in seen} == {"http://l/hook", "http://s/hook"}
    for _, content, h in seen:
        assert content == body and h["x-hub-signature-256"] == sig(body)
    assert {h["x-relay-mode"] for _, _, h in seen} == {"primary", "mirror"}


def test_phone_number_filter():
    c, _, _ = setup([Route("a", "http://a", phone_number_ids=frozenset({"111"})), Route("b", "http://b", phone_number_ids=frozenset({"222"}))], lambda r: httpx.Response(200))
    body = payload("222")
    assert c.post("/whatsapp", content=body, headers={"x-hub-signature-256": sig(body)}).json() == {"queued": 1}


def test_down_target_retries_and_does_not_block_other():
    calls = {"l": 0}

    def handler(r):
        if "l" == r.url.host:
            calls["l"] += 1
            return httpx.Response(200) if calls["l"] >= 3 else httpx.Response(503)
        return httpx.Response(200)

    c, store, d = setup([Route("laptop", "http://l/h"), Route("server", "http://s/h", "mirror")], handler)
    body = payload()
    c.post("/whatsapp", content=body, headers={"x-hub-signature-256": sig(body)})
    t = time.time()
    d.run_once(now=t)
    assert store.stats() == {"pending": 1, "done": 1}  # server done, laptop retrying
    d.run_once(now=t + 5)
    d.run_once(now=t + 60)
    assert store.stats() == {"done": 2} and calls["l"] == 3
