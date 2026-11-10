import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request, Response

from webhook_relay.config import Config, Provider, load
from webhook_relay.delivery import Deliverer, forwarded_headers
from webhook_relay.store import Store

log = logging.getLogger("relay")


def signature_ok(secret: str, body: bytes, header: str | None) -> bool:
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header.removeprefix("sha256="))


def phone_number_ids(body: bytes) -> set[str]:
    try:
        data = json.loads(body)
        return {
            str(c["value"]["metadata"]["phone_number_id"])
            for e in data.get("entry", [])
            for c in e.get("changes", [])
            if "phone_number_id" in c.get("value", {}).get("metadata", {})
        }
    except (ValueError, KeyError, TypeError, AttributeError):
        return set()


def create_app(config: Config, store: Store | None = None, deliverer: Deliverer | None = None, start_worker=False):
    store = store or Store(config.db_path)
    deliverer = deliverer or Deliverer(store, retain_hours=config.retain_hours)

    @asynccontextmanager
    async def lifespan(_app):
        if start_worker:
            deliverer.start()
        yield
        deliverer.stop()

    app = FastAPI(title="webhook-relay", lifespan=lifespan)
    app.state.store, app.state.deliverer = store, deliverer

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "queue": store.stats()}

    def add_provider(p: Provider):
        @app.get(p.path, name=f"{p.name}-verify")
        def verify(
            mode: str = Query(alias="hub.mode", default=""),
            token: str = Query(alias="hub.verify_token", default=""),
            challenge: str = Query(alias="hub.challenge", default=""),
        ):
            if mode == "subscribe" and p.verify_token and hmac.compare_digest(token, p.verify_token):
                return Response(challenge, media_type="text/plain")
            raise HTTPException(403, "verification failed")

        @app.post(p.path, name=f"{p.name}-receive")
        async def receive(request: Request):
            body = await request.body()
            if p.app_secret and not signature_ok(p.app_secret, body, request.headers.get("x-hub-signature-256")):
                raise HTTPException(401, "bad signature")
            ids = phone_number_ids(body)
            headers = forwarded_headers(request.headers)
            queued = 0
            for r in p.routes:
                if r.phone_number_ids and not (r.phone_number_ids & ids):
                    continue
                store.enqueue(p.name, r.name, r.url, r.mode, body, headers)
                queued += 1
            log.info("accepted provider=%s routes=%d bytes=%d", p.name, queued, len(body))
            return {"queued": queued}

    for p in config.providers:
        add_provider(p)
    return app


def from_env() -> FastAPI:
    return create_app(load(os.environ.get("RELAY_CONFIG", "/etc/relay/config.yaml")), start_worker=True)
