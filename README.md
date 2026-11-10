# webhook-relay

A small, durable webhook relay. Providers such as Meta allow one callback URL per app;
this gives you one public endpoint that verifies the request once and fans it out to
several targets: your laptop during development, a server, a mirror for observation.

- Answers the provider's verification challenge itself.
- Optionally verifies the `X-Hub-Signature-256` HMAC at the edge and drops forged requests.
- Responds 200 only after the event is written to a local SQLite queue.
- Forwards the **raw body and original signature header** unchanged, so each receiver can
  verify the signature itself. Adds `X-Relay-Route` and `X-Relay-Mode` (`primary`/`mirror`)
  so a mirror receiver can run observe-only and never reply twice.
- Routes by `phone_number_id` (optional), retries with backoff, never lets a dead target block
  the others, and purges delivered bodies after `retain_hours`. Logs never contain bodies.

```yaml
# config.yaml
providers:
  whatsapp:
    path: /whatsapp
    verify_token_env: WHATSAPP_VERIFY_TOKEN
    app_secret_env: WHATSAPP_APP_SECRET   # omit to skip signature checks at the relay
    routes:
      - {name: dev, url: "http://10.10.0.20:8080/webhooks/whatsapp", mode: primary}
      - {name: server, url: "http://app.internal/webhooks/whatsapp", mode: mirror}
```

```sh
uv run --with pytest pytest -q
RELAY_CONFIG=config.yaml uv run uvicorn webhook_relay.app:from_env --factory --port 8080
```

A runnable target is in [`examples/receiver`](examples/receiver): it verifies the forwarded signature, deduplicates by message ID, and treats `X-Relay-Mode: mirror` as observe-only. `tests/test_example_receiver.py` runs relay and receivers together.

Single replica only (SQLite on one volume). The Helm chart in `charts/webhook-relay` deploys a StatefulSet
with a persistent volume holding the retry queue and error records, retained if the set is deleted.
No delivery guarantee beyond at-least-once to each target; receivers must deduplicate.

Logging: the container runs uvicorn with `--no-access-log`, because webhook verification puts the verify token in the query string. The relay logs its own accepted-event lines (provider, route count, size) and never request bodies or query strings.
