import hashlib
import hmac
import importlib.util
import json
import pathlib

import httpx
from fastapi.testclient import TestClient

from webhook_relay.app import create_app as create_relay
from webhook_relay.config import Config, Provider, Route
from webhook_relay.delivery import Deliverer
from webhook_relay.store import Store

SECRET = "shared-secret"
spec = importlib.util.spec_from_file_location("sample_receiver", pathlib.Path(__file__).parents[1] / "examples/receiver/app.py")
sample = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sample)


def test_relay_to_sample_receivers_primary_acts_mirror_observes():
    primary, mirror = sample.create_app(SECRET), sample.create_app(SECRET)
    clients = {"p": TestClient(primary), "m": TestClient(mirror)}

    def forward(req: httpx.Request) -> httpx.Response:  # route relay deliveries into the in-process receivers
        r = clients["p" if req.url.host == "primary" else "m"].post(req.url.path, content=req.content, headers=dict(req.headers))
        return httpx.Response(r.status_code, json=r.json())

    cfg = Config(db_path=":memory:", providers=(Provider("whatsapp", "/whatsapp", "vt", SECRET, (
        Route("p", "http://primary/webhooks/whatsapp"), Route("m", "http://mirror/webhooks/whatsapp", "mirror"))),))
    store = Store(":memory:")
    deliverer = Deliverer(store, httpx.Client(transport=httpx.MockTransport(forward)))
    relay = TestClient(create_relay(cfg, store, deliverer))

    body = json.dumps({"entry": [{"changes": [{"value": {"metadata": {"phone_number_id": "1"}, "messages": [{"id": "m1"}]}}]}]}).encode()
    sig = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    for _ in range(2):  # the provider re-delivers the same event
        relay.post("/whatsapp", content=body, headers={"x-hub-signature-256": sig})
        deliverer.run_once()

    assert primary.state.handled == ["m1"] and primary.state.observed == []
    assert mirror.state.observed == ["m1"] and mirror.state.handled == []  # mirror never acts; duplicate ignored
    assert store.stats() == {"done": 4}
