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
- Routes by `phone_number_id` (optional), retries with backoff, delivers to every route concurrently so a dead or
  slow target cannot delay the others (see "Delivery model"), and purges delivered bodies after `retain_hours`.
  Logs never contain bodies.

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

Optional per-route settings (defaults depend on `mode`) and a global `lag_warn_seconds`:

| key | primary | mirror | meaning |
|---|---|---|---|
| `connect_timeout` | 5 s | 2 s | TCP connect timeout |
| `timeout` | 30 s | 5 s | write / read-response timeout (applied to each phase, not a hard total) |
| `workers` | 8 | 2 | concurrent deliveries to this route |
| `breaker_failures` | 0 (off) | 3 | consecutive connect failures that open the circuit |
| `breaker_cooldown` | 60 s | 60 s | seconds before a probe |

`lag_warn_seconds` (top level, default 30) is the lag threshold for the slow-delivery warning.

```sh
uv run --with pytest pytest -q
RELAY_CONFIG=config.yaml uv run uvicorn webhook_relay.app:from_env --factory --port 8080
```

A runnable target is in [`examples/receiver`](examples/receiver): it verifies the forwarded signature, deduplicates by message ID, and treats `X-Relay-Mode: mirror` as observe-only. `tests/test_example_receiver.py` runs relay and receivers together.

Default: single replica (SQLite on one volume). For N replicas use the PostgreSQL store (see "High availability"). The Helm chart in `charts/webhook-relay` deploys a StatefulSet
with a persistent volume holding the retry queue and error records, retained if the set is deleted.
No delivery guarantee beyond at-least-once to each target; receivers must deduplicate.

Multiple Meta apps can share the same callback path. Configure a provider per app with distinct signing secrets and
verify tokens. GET verification accepts each provider's token; POST delivery selects the provider by its valid HMAC,
then considers only that provider's routes. Use phone-number filters to limit each route to its intended sender.
Shared paths reject empty or duplicate app secrets. The Helm chart's `extraEnv` accepts additional `secretKeyRef`
environment entries; never put secret values in Helm values or Git.

Logging: the container runs uvicorn with `--no-access-log`, because webhook verification puts the verify token in the query string. The relay logs its own accepted-event lines (provider, route count, size) and never request bodies or query strings.

## Delivery model

**Routing.** Each accepted event is queued once per matching route (`match.phone_number_id`, or all events when
omitted). Success means any 2xx. A primary route makes up to 8 attempts (backoff 2 s, 10 s, 30 s, 2 min, 5 min,
15 min, 30 min, 1 h); a mirror makes 3. A row that runs out of attempts is `dead`, logged, and kept for `retain_hours`.

**Concurrency.** A dispatcher claims due rows and hands them to a worker pool owned by each route
(`workers`, see above). A route can therefore only exhaust its own workers: a mirror that hangs or is powered off
never delays a primary, and a slow primary never delays another route or another target. Different business numbers
and different WhatsApp users are delivered in parallel.

**Ordering.** Events carry a lane key: the contact in the payload (`messages[].from` / `statuses[].recipient_id`).
For each (route, sender) only the oldest unfinished row is eligible, so a sender's events reach a route in the order
Meta sent them, one at a time. While that delivery is failing and backing off, that sender's later events wait on
that route; other senders and other routes continue. Events without a sender are not ordered. A webhook batching
several contacts forms one lane for that exact set of contacts. Delivery stays at-least-once; receivers deduplicate.

**Claiming and restart.** Rows move `pending -> inflight -> done|dead` (or back to `pending` on retry) under the
store lock, so a row is delivered by at most one worker at a time. Rows left `inflight` by a crash or restart are
returned to `pending` at startup and retried. Single replica only; SQLite runs in WAL mode.

**Circuit breaker.** After `breaker_failures` consecutive connect failures (refused / connect timeout) a route's
circuit opens for `breaker_cooldown` seconds. While open, a mirror's queued and new rows are marked `dead`
immediately with `err=circuit_open`, with no network call. A primary with the breaker enabled keeps its rows
`pending` instead. After the cooldown one probe request is sent; if it connects the circuit closes, otherwise it
reopens. A powered-off laptop therefore costs a few short connect timeouts, then essentially nothing.

## Log lines

Logs never contain bodies or query strings.

- `accepted provider=<p> routes=<n> bytes=<n>`: event queued.
- `dead route=<name> id=<id> err=<error>`: delivery gave up (attempts exhausted, or `circuit_open`). Alert on this.
- `slow delivery route=<name> id=<id> lag=<s>s outcome=<done|dead>`: finished more than `lag_warn_seconds` after it was created.
- `circuit open route=<name> failures=<n> cooldown=<s>s`: a route's breaker opened.
- `recovered <n> inflight deliveries after restart`.

## High availability (PostgreSQL store)

The default store is SQLite and stays single-replica. Set `RELAY_STORE_URL=postgresql://...` to use PostgreSQL instead
(install the extra: `pip install webhook-relay[postgres]`, or `docker build --build-arg EXTRAS=[postgres] .`). Then any
number of replicas can share the queue:

- Enqueue commits before the 200, as with SQLite. Claiming uses `FOR UPDATE SKIP LOCKED` plus a lease
  (`locked_until`); a per-route advisory lock keeps per-sender lanes strictly ordered across replicas.
- Recovery is lease-aware: a restarting replica only re-queues rows whose lease expired, so it never takes a live
  replica's work. A dead replica's rows come back after `RELAY_LEASE_SECONDS` (default 120; keep it above connect + 3x
  timeout of your slowest route). Delivery stays at-least-once.
- Schema creation at start-up is guarded by an advisory lock, so replicas can start together.
- `/readyz` checks the store (503 when unreachable); `/livez` does not touch it.

Helm: `store.type=postgres`, `store.urlSecret.name` (existing Secret holding the URL, key `store-url`), `replicas`, and
`podDisruptionBudget` (minAvailable 1) render a Deployment with no PVC. `replicas>1` with the default `sqlite` fails
the render. The StatefulSet path is unchanged.

| setting (env, or yaml key) | default | meaning |
|---|---|---|
| `RELAY_STORE_URL` | unset (SQLite at `db_path`) | `postgresql://` URL selects the Postgres store |
| `RELAY_LEASE_SECONDS` | 120 | claim lease (Postgres) |
| `RELAY_MAX_ATTEMPTS` / `max_attempts` | primary 8, mirror 3 | `12` (primary), or `primary=12,mirror=3` |
| `RELAY_BACKOFF` / `backoff` | 2,10,30,120,300,900,1800,3600 | seconds between attempts, last value repeats |
| `RELAY_DEAD_RETAIN_HOURS` / `dead_retain_hours` | = `retain_hours` | how long dead rows keep their bodies |

Dead-letter replay: `webhook-relay replay --dead [--route NAME]` requeues dead rows with a fresh retry budget (run it
where the relay runs, same env; e.g. `kubectl exec`). Caveat: ids of a lane are ordered by commit; if a lane is
replayed while newer events for that sender are pending, the replayed (older) row is delivered after them only if
those already completed, otherwise it is delivered first since its id is smaller.
