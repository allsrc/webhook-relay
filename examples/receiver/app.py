"""Sample webhook receiver for webhook-relay targets.

Shows the three things every target should do:
  1. verify the provider's HMAC signature itself (the relay forwards it untouched),
  2. deduplicate by message ID (delivery is at-least-once, retries can repeat),
  3. honour X-Relay-Mode: a `mirror` receiver observes only and must never reply.

Run:  APP_SECRET=... uvicorn app:create_app --factory --port 8080
"""
import hashlib
import hmac
import json
import os

from fastapi import FastAPI, HTTPException, Request


def signature_ok(secret: str, body: bytes, header: str | None) -> bool:
    if not secret or not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header.removeprefix("sha256="))


def message_ids(payload: dict) -> list[str]:
    return [
        m["id"]
        for e in payload.get("entry", [])
        for c in e.get("changes", [])
        for m in c.get("value", {}).get("messages", [])
    ]


def create_app(secret: str | None = None) -> FastAPI:
    secret = secret if secret is not None else os.environ.get("APP_SECRET", "")
    app = FastAPI(title="sample receiver")
    seen: set[str] = set()
    app.state.handled, app.state.observed = [], []

    @app.post("/webhooks/whatsapp")
    async def receive(request: Request):
        body = await request.body()
        if not signature_ok(secret, body, request.headers.get("x-hub-signature-256")):
            raise HTTPException(401, "bad signature")
        mode = request.headers.get("x-relay-mode", "primary")
        fresh = [i for i in message_ids(json.loads(body)) if i not in seen]
        seen.update(fresh)
        if mode == "mirror":
            app.state.observed.extend(fresh)  # record only: no replies, no side effects
        else:
            app.state.handled.extend(fresh)  # your real handling (and any replies) goes here
        return {"new": len(fresh), "mode": mode}

    return app
