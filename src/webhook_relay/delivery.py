import json
import logging
import threading
import time

import httpx

from webhook_relay.store import Store

log = logging.getLogger("relay")
BACKOFF = [2, 10, 30, 120, 300, 900, 1800, 3600]
MAX_ATTEMPTS = {"primary": 8, "mirror": 3}
FORWARDED = ("content-type", "x-hub-signature-256", "x-hub-signature", "user-agent")


class Deliverer:
    def __init__(self, store: Store, client: httpx.Client | None = None, retain_hours: int = 24):
        self.store, self.client, self.retain = store, client or httpx.Client(timeout=10), retain_hours * 3600
        self._stop = threading.Event()

    def run_once(self, now: float | None = None) -> int:
        now = now if now is not None else time.time()
        n = 0
        for row in self.store.due(now):
            n += 1
            headers = json.loads(row["headers"]) | {"x-relay-route": row["route"], "x-relay-mode": row["mode"]}
            attempts = row["attempts"] + 1
            try:
                r = self.client.post(row["url"], content=row["body"], headers=headers)
                ok, err = r.status_code < 300, f"HTTP {r.status_code}"
            except httpx.HTTPError as exc:
                ok, err = False, type(exc).__name__
            if ok:
                self.store.mark_done(row["id"], now)
            elif attempts >= MAX_ATTEMPTS.get(row["mode"], 3):
                self.store.mark_dead(row["id"], attempts, now, err)
                log.warning("dead route=%s id=%s err=%s", row["route"], row["id"], err)
            else:
                self.store.mark_retry(row["id"], attempts, now + BACKOFF[min(attempts - 1, len(BACKOFF) - 1)], err)
        self.store.purge(now, self.retain)
        return n

    def start(self):
        def loop():
            while not self._stop.is_set():
                try:
                    if not self.run_once():
                        self._stop.wait(1.0)
                except Exception:  # keep the worker alive; details never include bodies
                    log.exception("delivery loop error")
                    self._stop.wait(2.0)

        threading.Thread(target=loop, daemon=True, name="deliverer").start()

    def stop(self):
        self._stop.set()


def forwarded_headers(request_headers) -> dict:
    return {k: v for k, v in ((h, request_headers.get(h)) for h in FORWARDED) if v}
