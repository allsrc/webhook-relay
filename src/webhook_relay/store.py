import json
import sqlite3
import threading
import time


class Store:
    """Durable delivery queue. Bodies are kept only until delivered/dead + retention.

    One SQLite connection (WAL) guarded by a lock, so it is safe to use from the HTTP
    handlers, the dispatcher and every delivery worker. Row states:
    pending -> inflight (claimed by a worker) -> done | dead, or back to pending on retry.
    """

    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS deliveries(
            id INTEGER PRIMARY KEY, provider TEXT, route TEXT, url TEXT, mode TEXT,
            body BLOB, headers TEXT, attempts INTEGER DEFAULT 0, next_at REAL,
            status TEXT DEFAULT 'pending', created REAL, finished REAL, last_error TEXT)"""
        )
        cols = {r["name"] for r in self._db.execute("PRAGMA table_info(deliveries)")}
        if "sender" not in cols:  # migration for databases created by older versions
            self._db.execute("ALTER TABLE deliveries ADD COLUMN sender TEXT DEFAULT ''")
        if "claimed" not in cols:
            self._db.execute("ALTER TABLE deliveries ADD COLUMN claimed REAL")
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS deliveries_lane ON deliveries(provider, route, sender, status, id)"
        )
        self._db.commit()
        self._lock = threading.Lock()

    def enqueue(self, provider, route, url, mode, body: bytes, headers: dict, now: float | None = None,
                sender: str = "") -> int:
        now = now if now is not None else time.time()
        with self._lock, self._db:
            cur = self._db.execute(
                "INSERT INTO deliveries(provider,route,url,mode,body,headers,next_at,created,sender)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (provider, route, url, mode, body, json.dumps(headers), now, now, sender or ""),
            )
            return cur.lastrowid

    def due_routes(self, now: float) -> list[tuple[str, str, str]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT DISTINCT provider, route, mode FROM deliveries WHERE status='pending' AND next_at<=?", (now,)
            ).fetchall()
        return [(r["provider"], r["route"], r["mode"]) for r in rows]

    def claim(self, provider: str, route: str, now: float, limit: int) -> list[sqlite3.Row]:
        """Atomically move up to `limit` due rows of one route to 'inflight' and return them.

        Only the oldest unfinished row of each (route, sender) lane is eligible, which keeps a
        sender's events in order; rows without a sender are not ordered.
        """
        if limit <= 0:
            return []
        with self._lock, self._db:
            rows = self._db.execute(
                """SELECT d.* FROM deliveries d
                   WHERE d.provider=? AND d.route=? AND d.status='pending' AND d.next_at<=?
                     AND (d.sender='' OR d.sender IS NULL OR NOT EXISTS (
                         SELECT 1 FROM deliveries e
                         WHERE e.provider=d.provider AND e.route=d.route AND e.sender=d.sender
                           AND e.id<d.id AND e.status IN ('pending','inflight')))
                   ORDER BY d.id LIMIT ?""",
                (provider, route, now, limit),
            ).fetchall()
            claimed = []
            for r in rows:
                cur = self._db.execute(
                    "UPDATE deliveries SET status='inflight', claimed=? WHERE id=? AND status='pending'", (now, r["id"])
                )
                if cur.rowcount == 1:
                    claimed.append(r)
            return claimed

    def recover_inflight(self, now: float | None = None) -> int:
        """Release rows left 'inflight' by a crash/restart so they are retried (at-least-once)."""
        now = now if now is not None else time.time()
        with self._lock, self._db:
            return self._db.execute(
                "UPDATE deliveries SET status='pending', next_at=?, claimed=NULL WHERE status='inflight'", (now,)
            ).rowcount

    def release(self, id_: int, now: float):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE deliveries SET status='pending', next_at=?, claimed=NULL WHERE id=? AND status='inflight'",
                (now, id_),
            )

    def mark_done(self, id_: int, now: float):
        with self._lock, self._db:
            self._db.execute("UPDATE deliveries SET status='done', finished=?, body=NULL WHERE id=?", (now, id_))

    def mark_retry(self, id_: int, attempts: int, next_at: float, error: str):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE deliveries SET status='pending', claimed=NULL, attempts=?, next_at=?, last_error=? WHERE id=?",
                (attempts, next_at, error, id_),
            )

    def mark_dead(self, id_: int, attempts: int, now: float, error: str):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE deliveries SET status='dead', attempts=?, finished=?, last_error=? WHERE id=?",
                (attempts, now, error, id_),
            )

    def kill_pending(self, provider: str, route: str, now: float, error: str) -> list[int]:
        """Mark every not-yet-claimed row of a route dead (used while its circuit is open)."""
        with self._lock, self._db:
            ids = [
                r["id"]
                for r in self._db.execute(
                    "SELECT id FROM deliveries WHERE provider=? AND route=? AND status='pending'", (provider, route)
                )
            ]
            self._db.executemany(
                "UPDATE deliveries SET status='dead', finished=?, last_error=? WHERE id=?",
                [(now, error, i) for i in ids],
            )
            return ids

    def purge(self, now: float, retain_seconds: float):
        with self._lock, self._db:
            self._db.execute("DELETE FROM deliveries WHERE finished IS NOT NULL AND finished<?", (now - retain_seconds,))

    def stats(self) -> dict:
        with self._lock:
            rows = self._db.execute("SELECT status, COUNT(*) c FROM deliveries GROUP BY status").fetchall()
        return {r["status"]: r["c"] for r in rows}

    def get(self, id_: int) -> sqlite3.Row | None:
        with self._lock:
            return self._db.execute("SELECT * FROM deliveries WHERE id=?", (id_,)).fetchone()
