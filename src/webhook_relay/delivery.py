import json
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

import httpx

from webhook_relay.config import Route
from webhook_relay.store import QueueStore

log = logging.getLogger("relay")
BACKOFF = [2, 10, 30, 120, 300, 900, 1800, 3600]
MAX_ATTEMPTS = {"primary": 8, "mirror": 3}
FORWARDED = ("content-type", "x-hub-signature-256", "x-hub-signature", "user-agent")
CONNECT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)


@dataclass(frozen=True)
class RouteSettings:
    connect_timeout: float
    timeout: float
    workers: int
    breaker_failures: int  # 0 = circuit breaker disabled
    breaker_cooldown: float

    @classmethod
    def for_route(cls, mode: str, route: Route | None = None) -> "RouteSettings":
        d = cls(2.0, 5.0, 2, 3, 60.0) if mode == "mirror" else cls(5.0, 30.0, 8, 0, 60.0)
        if route is None:
            return d

        def pick(v, default):
            return default if v is None else v

        return cls(
            pick(route.connect_timeout, d.connect_timeout),
            pick(route.timeout, d.timeout),
            max(1, pick(route.workers, d.workers)),
            pick(route.breaker_failures, d.breaker_failures),
            pick(route.breaker_cooldown, d.breaker_cooldown),
        )


class Breaker:
    """Per-route circuit breaker on consecutive connect failures: closed -> open -> half-open probe."""

    def __init__(self, settings: RouteSettings):
        self.s = settings
        self.failures = 0
        self.open_until = 0.0
        self.probing = False
        self._lock = threading.Lock()

    def state(self, now: float) -> str:
        """closed | open (cooling down) | probe (send one request) | wait (probe in flight)"""
        with self._lock:
            if self.s.breaker_failures <= 0 or self.open_until == 0.0:
                return "closed"
            if now < self.open_until:
                return "open"
            return "wait" if self.probing else "probe"

    def begin_probe(self):
        with self._lock:
            self.probing = True

    def success(self):
        with self._lock:
            self.failures, self.open_until, self.probing = 0, 0.0, False

    def connect_failure(self, now: float) -> bool:
        with self._lock:
            self.failures += 1
            self.probing = False
            if self.s.breaker_failures > 0 and self.failures >= self.s.breaker_failures:
                self.open_until = now + self.s.breaker_cooldown
                return True
            return False

    def neutral(self):
        """Connected but failed another way, or the worker aborted: not evidence the host is down."""
        with self._lock:
            self.probing = False
            self.failures, self.open_until = 0, 0.0


class Deliverer:
    """Dispatches queued deliveries concurrently.

    A dispatcher thread claims due rows per route and hands them to that route's own worker pool,
    so a slow or dead route only ever occupies its own workers. The store releases at most one
    unfinished row per (route, sender), which preserves a sender's event order within a route.
    """

    def __init__(self, store: QueueStore, client: httpx.Client | None = None, retain_hours: int = 24,
                 routes: dict[tuple[str, str], Route] | None = None, lag_warn_seconds: float = 30.0,
                 max_attempts: dict[str, int] | None = None, backoff: list[float] | None = None,
                 dead_retain_hours: float | None = None):
        self.store = store
        self.max_attempts = MAX_ATTEMPTS | (max_attempts or {})
        self.backoff = list(backoff) if backoff else BACKOFF
        self.dead_retain = None if dead_retain_hours is None else dead_retain_hours * 3600
        self.client = client or httpx.Client(
            timeout=10, limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
        )
        self.retain = retain_hours * 3600
        self.routes = routes or {}
        self.lag_warn = lag_warn_seconds
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._pools: dict[tuple[str, str], ThreadPoolExecutor] = {}
        self._active: dict[tuple[str, str], int] = {}
        self._breakers: dict[tuple[str, str], Breaker] = {}
        self._settings: dict[tuple[str, str], RouteSettings] = {}
        self._thread: threading.Thread | None = None
        self._last_purge = 0.0
        self._last_recover = 0.0

    def settings(self, key, mode) -> RouteSettings:
        with self._lock:
            if key not in self._settings:
                self._settings[key] = RouteSettings.for_route(mode, self.routes.get(key))
            return self._settings[key]

    def breaker(self, key, mode) -> Breaker:
        s = self.settings(key, mode)
        with self._lock:
            return self._breakers.setdefault(key, Breaker(s))

    def _pool(self, key, workers) -> ThreadPoolExecutor:
        with self._lock:
            if key not in self._pools:
                self._pools[key] = ThreadPoolExecutor(workers, thread_name_prefix=f"deliver-{key[1]}")
            return self._pools[key]

    def wake(self):
        self._wake.set()

    def recover(self, now: float | None = None) -> int:
        """Re-queue rows left inflight by a previous process (SQLite: all; Postgres: only expired leases)."""
        n = self.store.recover_inflight(now)
        if n:
            log.warning("recovered %d inflight deliveries after restart", n)
        return n

    def dispatch(self, now: float | None = None) -> list[Future]:
        """One scheduling pass: claim due rows per route and submit them to that route's pool."""
        t = now if now is not None else time.time()
        futures: list[Future] = []
        for provider, route, mode in self.store.due_routes(t):
            key = (provider, route)
            s = self.settings(key, mode)
            br = self.breaker(key, mode)
            state = br.state(t)
            if state == "wait":
                continue
            if state == "open":
                if mode == "mirror":  # cheap fail: never queue work behind a dead mirror
                    for id_ in self.store.kill_pending(provider, route, t, "circuit_open"):
                        log.warning("dead route=%s id=%s err=%s", route, id_, "circuit_open")
                continue  # primary: rows stay pending until the probe window
            with self._lock:
                capacity = s.workers - self._active.get(key, 0)
            if state == "probe":
                capacity = min(capacity, 1)
            rows = self.store.claim(provider, route, t, capacity)
            if rows and state == "probe":
                br.begin_probe()
            pool = self._pool(key, s.workers)
            for row in rows:
                with self._lock:
                    self._active[key] = self._active.get(key, 0) + 1
                futures.append(pool.submit(self._work, key, row, s, br, now))
        return futures

    def run_once(self, now: float | None = None) -> int:
        """Synchronous pass (tests): dispatch and wait until nothing more is due."""
        n = 0
        while True:
            futures = self.dispatch(now)
            if not futures:
                break
            n += len(futures)
            wait(futures)
        self.store.purge(now if now is not None else time.time(), self.retain, self.dead_retain)
        return n

    def _work(self, key, row, s: RouteSettings, br: Breaker, base: float | None):
        try:
            self._deliver(row, s, br, base)
        except Exception:
            br.neutral()
            self.store.release(row["id"], time.time() if base is None else base)
            log.exception("delivery worker error route=%s id=%s", row["route"], row["id"])
        finally:
            with self._lock:
                self._active[key] -= 1
            self._wake.set()

    def _deliver(self, row, s: RouteSettings, br: Breaker, base: float | None):
        headers = json.loads(row["headers"]) | {"x-relay-route": row["route"], "x-relay-mode": row["mode"]}
        attempts = row["attempts"] + 1
        connect_failed = False
        try:
            timeout = httpx.Timeout(connect=s.connect_timeout, read=s.timeout, write=s.timeout, pool=s.timeout)
            with self.client.stream("POST", row["url"], content=row["body"], headers=headers, timeout=timeout) as r:
                ok, err = r.status_code < 300, f"HTTP {r.status_code}"
        except httpx.HTTPError as exc:
            ok, err = False, type(exc).__name__
            connect_failed = isinstance(exc, CONNECT_ERRORS)
        now = base if base is not None else time.time()
        if connect_failed:
            if br.connect_failure(now):
                log.warning("circuit open route=%s failures=%d cooldown=%ss", row["route"], br.failures, br.s.breaker_cooldown)
        elif ok:
            br.success()
        else:
            br.neutral()
        if ok:
            self.store.mark_done(row["id"], now)
            self._lag(row, now, "done")
        elif attempts >= self.max_attempts.get(row["mode"], 3):
            self.store.mark_dead(row["id"], attempts, now, err)
            log.warning("dead route=%s id=%s err=%s", row["route"], row["id"], err)
            self._lag(row, now, "dead")
        else:
            self.store.mark_retry(row["id"], attempts, now + self.backoff[min(attempts - 1, len(self.backoff) - 1)], err)

    def _lag(self, row, finished: float, outcome: str):
        lag = finished - (row["created"] or finished)
        if lag > self.lag_warn:
            log.warning("slow delivery route=%s id=%s lag=%.1fs outcome=%s", row["route"], row["id"], lag, outcome)

    def start(self):
        self.recover()

        def loop():
            while not self._stop.is_set():
                try:
                    if time.time() - self._last_recover > 15:  # a crashed replica's expired leases
                        self._last_recover = time.time()
                        n = self.store.recover_expired(self._last_recover)
                        if n:
                            log.warning("recovered %d deliveries with expired leases", n)
                    self.dispatch()
                    if time.time() - self._last_purge > 60:
                        self._last_purge = time.time()
                        self.store.purge(self._last_purge, self.retain, self.dead_retain)
                except Exception:  # keep the dispatcher alive; details never include bodies
                    log.exception("delivery loop error")
                    self._stop.wait(2.0)
                self._wake.wait(0.5)
                self._wake.clear()

        self._thread = threading.Thread(target=loop, daemon=True, name="dispatcher")
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._wake.set()
        with self._lock:
            pools = list(self._pools.values())
        for p in pools:
            p.shutdown(wait=False, cancel_futures=True)


def forwarded_headers(request_headers) -> dict:
    return {k: v for k, v in ((h, request_headers.get(h)) for h in FORWARDED) if v}
