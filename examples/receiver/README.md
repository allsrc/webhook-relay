# Sample receiver

A minimal target for webhook-relay. It verifies the forwarded `X-Hub-Signature-256`
itself, ignores repeated message IDs, and treats `X-Relay-Mode: mirror` as observe-only
so two receivers never answer the same user twice.

```sh
cd examples/receiver
APP_SECRET=<your Meta app secret> uv run --with fastapi --with uvicorn uvicorn app:create_app --factory --port 8080
```

Point a relay route at `http://<this-host>:8080/webhooks/whatsapp`.
