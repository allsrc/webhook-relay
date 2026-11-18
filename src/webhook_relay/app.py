import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request, Response

from webhook_relay.config import Config, Provider, load
from webhook_relay.delivery import Deliverer, forwarded_headers
from webhook_relay.store import QueueStore, open_store

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


def sender_lane(body: bytes) -> str:
    """The WhatsApp contact(s) an event concerns; events for one sender are delivered in order."""
    try:
        data = json.loads(body)
        found = set()
        for e in data.get("entry", []):
            for c in e.get("changes", []):
                v = c.get("value", {})
                found.update(str(m["from"]) for m in v.get("messages", []) if "from" in m)
                found.update(str(x["recipient_id"]) for x in v.get("statuses", []) if "recipient_id" in x)
        return "|".join(sorted(found))
    except (ValueError, KeyError, TypeError, AttributeError):
        return ""


def create_app(config: Config, store: QueueStore | None = None, deliverer: Deliverer | None = None, start_worker=False):
    store = store or open_store(config.db_path)
    routes = {(p.name, r.name): r for p in config.providers for r in p.routes}
    deliverer = deliverer or Deliverer(
        store, retain_hours=config.retain_hours, routes=routes, lag_warn_seconds=config.lag_warn_seconds,
        max_attempts=dict(config.max_attempts), backoff=list(config.backoff) or None,
        dead_retain_hours=config.dead_retain_hours,
    )

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

    @app.get("/livez")
    def livez():  # process liveness only; never touches the store, so a database blip does not restart pods
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():
        try:
            store.ping()
        except Exception:
            log.exception("store not ready")
            raise HTTPException(503, "store unavailable")
        return {"status": "ready"}

    def add_endpoint(path: str, providers: tuple[Provider, ...]):
        # A shared callback selects the app by its signature, before considering routes.
        # An unsigned fallback here would bypass every other app's access boundary.
        if len(providers) > 1:
            secrets = [p.app_secret for p in providers]
            if not all(secrets) or len(set(secrets)) != len(secrets):
                raise ValueError("providers sharing a path require distinct, nonempty app secrets")

        @app.get(path, name=f"{providers[0].name}-verify")
        def verify(
            mode: str = Query(alias="hub.mode", default=""),
            token: str = Query(alias="hub.verify_token", default=""),
            challenge: str = Query(alias="hub.challenge", default=""),
        ):
            if mode == "subscribe" and any(
                p.verify_token and hmac.compare_digest(token, p.verify_token) for p in providers
            ):
                return Response(challenge, media_type="text/plain")
            raise HTTPException(403, "verification failed")

        @app.post(path, name=f"{providers[0].name}-receive")
        async def receive(request: Request):
            body = await request.body()
            header = request.headers.get("x-hub-signature-256")
            p = next((p for p in providers if p.app_secret and signature_ok(p.app_secret, body, header)), None)
            if p is None and len(providers) == 1 and not providers[0].app_secret:
                p = providers[0]  # retain the explicitly unsigned single-provider mode
            if p is None:
                raise HTTPException(401, "bad signature")
            ids = phone_number_ids(body)
            sender = sender_lane(body)
            headers = forwarded_headers(request.headers)
            queued = 0
            for r in p.routes:
                if r.phone_number_ids and not (r.phone_number_ids & ids):
                    continue
                store.enqueue(p.name, r.name, r.url, r.mode, body, headers, sender=sender)
                queued += 1
            if queued:
                deliverer.wake()
            log.info("accepted provider=%s routes=%d bytes=%d", p.name, queued, len(body))
            return {"queued": queued}

    paths: dict[str, list[Provider]] = {}
    for p in config.providers:
        paths.setdefault(p.path, []).append(p)
    for path, providers in paths.items():
        add_endpoint(path, tuple(providers))
    return app


def from_env() -> FastAPI:
    return create_app(load(os.environ.get("RELAY_CONFIG", "/etc/relay/config.yaml")), start_worker=True)
