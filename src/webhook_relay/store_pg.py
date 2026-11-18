"""PostgreSQL delivery queue: N relay replicas share one database.

Requires the optional extra: `pip install webhook-relay[postgres]` (psycopg 3 + psycopg_pool).

Claiming
    One transaction per (provider, route): a transaction-scoped advisory lock serializes claimers of that
    route across replicas, expired leases are released, then the oldest unfinished row of each sender lane
    is selected `FOR UPDATE SKIP LOCKED` and moved to `inflight` with `locked_until = now + lease`.
    The lock keeps a sender's lane strictly ordered (sender B's message N+1 is never claimed while N is
    pending or inflight on any replica); SKIP LOCKED keeps claimers from ever waiting on a row lock.
Enqueue
    Committed before the HTTP 200 (same ack semantics as SQLite). Rows of one lane are inserted under a
    per-lane advisory lock so row ids follow commit order, which the lane query relies on.
Leases
    A replica that dies mid-delivery leaves `inflight` rows; once `locked_until` passes, any replica returns
    them to `pending` (at-least-once). Unlike SQLite's start-up `recover_inflight`, a live replica's
    claims are never taken. Choose a lease longer than the slowest delivery (connect + 3 x timeout).
"""
import json
import time

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_SCHEMA_LOCK = 7_261_000_001  # arbitrary app-wide advisory lock key for schema creation

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS deliveries(
        id BIGSERIAL PRIMARY KEY, provider TEXT, route TEXT, url TEXT, mode TEXT,
        body BYTEA, headers TEXT, attempts INTEGER DEFAULT 0, next_at DOUBLE PRECISION,
        status TEXT DEFAULT 'pending', created DOUBLE PRECISION, finished DOUBLE PRECISION,
        last_error TEXT, sender TEXT DEFAULT '', claimed DOUBLE PRECISION, locked_until DOUBLE PRECISION)""",
    "CREATE INDEX IF NOT EXISTS deliveries_lane ON deliveries(provider, route, sender, status, id)",
    "CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(provider, route, status, next_at)",
    "CREATE INDEX IF NOT EXISTS deliveries_finished ON deliveries(finished) WHERE finished IS NOT NULL",
]


class PostgresStore:
    def __init__(self, url: str, lease_seconds: float = 120.0, max_pool: int = 20):
        self.lease = lease_seconds
        self._pool = ConnectionPool(
            url, min_size=1, max_size=max_pool, kwargs={"row_factory": dict_row}, open=True, timeout=30
        )
        self._pool.wait()
        with self._pool.connection() as c:  # replicas may start together: one creates, the rest wait
            c.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK,))
            for stmt in _SCHEMA:
                c.execute(stmt)

    def close(self):
        self._pool.close()

    def ping(self) -> None:
        with self._pool.connection() as c:
            c.execute("SELECT 1")

    def enqueue(self, provider, route, url, mode, body: bytes, headers: dict, now: float | None = None,
                sender: str = "") -> int:
        now = now if now is not None else time.time()
        with self._pool.connection() as c:  # commits on exit, before the caller answers 200
            if sender:
                c.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{provider}/{route}/{sender}",))
            return c.execute(
                "INSERT INTO deliveries(provider,route,url,mode,body,headers,next_at,created,sender)"
                " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (provider, route, url, mode, body, json.dumps(headers), now, now, sender or ""),
            ).fetchone()["id"]

    def due_routes(self, now: float) -> list[tuple[str, str, str]]:
        with self._pool.connection() as c:
            rows = c.execute(
                "SELECT DISTINCT provider, route, mode FROM deliveries WHERE status='pending' AND next_at<=%s", (now,)
            ).fetchall()
        return [(r["provider"], r["route"], r["mode"]) for r in rows]

    def claim(self, provider: str, route: str, now: float, limit: int) -> list[dict]:
        if limit <= 0:
            return []
        with self._pool.connection() as c:
            c.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 1))", (f"{provider}/{route}",))
            c.execute(  # lease-aware: only rows whose holder stopped renewing/finishing
                "UPDATE deliveries SET status='pending', claimed=NULL, locked_until=NULL, next_at=%s"
                " WHERE provider=%s AND route=%s AND status='inflight' AND (locked_until IS NULL OR locked_until<%s)",
                (now, provider, route, now),
            )
            ids = [
                r["id"]
                for r in c.execute(
                    """SELECT d.id FROM deliveries d
                       WHERE d.provider=%s AND d.route=%s AND d.status='pending' AND d.next_at<=%s
                         AND (d.sender='' OR d.sender IS NULL OR NOT EXISTS (
                             SELECT 1 FROM deliveries e
                             WHERE e.provider=d.provider AND e.route=d.route AND e.sender=d.sender
                               AND e.id<d.id AND e.status IN ('pending','inflight')))
                       ORDER BY d.id LIMIT %s FOR UPDATE OF d SKIP LOCKED""",
                    (provider, route, now, limit),
                )
            ]
            if not ids:
                return []
            rows = c.execute(
                "UPDATE deliveries SET status='inflight', claimed=%s, locked_until=%s WHERE id = ANY(%s) RETURNING *",
                (now, now + self.lease, ids),
            ).fetchall()
        return sorted(rows, key=lambda r: r["id"])

    def recover_expired(self, now: float | None = None) -> int:
        now = now if now is not None else time.time()
        with self._pool.connection() as c:
            return c.execute(
                "UPDATE deliveries SET status='pending', next_at=%s, claimed=NULL, locked_until=NULL"
                " WHERE status='inflight' AND (locked_until IS NULL OR locked_until<%s)",
                (now, now),
            ).rowcount

    def recover_inflight(self, now: float | None = None) -> int:
        """Start-up recovery. Lease-aware: other replicas' live claims are left alone."""
        return self.recover_expired(now)

    def release(self, id_: int, now: float):
        self._exec(
            "UPDATE deliveries SET status='pending', next_at=%s, claimed=NULL, locked_until=NULL"
            " WHERE id=%s AND status='inflight'", (now, id_))

    def mark_done(self, id_: int, now: float):
        self._exec(
            "UPDATE deliveries SET status='done', finished=%s, body=NULL, locked_until=NULL WHERE id=%s", (now, id_))

    def mark_retry(self, id_: int, attempts: int, next_at: float, error: str):
        self._exec(
            "UPDATE deliveries SET status='pending', claimed=NULL, locked_until=NULL, attempts=%s, next_at=%s,"
            " last_error=%s WHERE id=%s", (attempts, next_at, error, id_))

    def mark_dead(self, id_: int, attempts: int, now: float, error: str):
        self._exec(
            "UPDATE deliveries SET status='dead', attempts=%s, finished=%s, last_error=%s, locked_until=NULL"
            " WHERE id=%s", (attempts, now, error, id_))

    def kill_pending(self, provider: str, route: str, now: float, error: str) -> list[int]:
        with self._pool.connection() as c:
            return [
                r["id"]
                for r in c.execute(
                    "UPDATE deliveries SET status='dead', finished=%s, last_error=%s"
                    " WHERE provider=%s AND route=%s AND status='pending' RETURNING id",
                    (now, error, provider, route),
                )
            ]

    def requeue_dead(self, route: str | None = None, now: float | None = None) -> int:
        now = now if now is not None else time.time()
        sql = ("UPDATE deliveries SET status='pending', attempts=0, next_at=%s, finished=NULL, claimed=NULL,"
               " locked_until=NULL WHERE status='dead'")
        args: list = [now]
        if route:
            sql += " AND route=%s"
            args.append(route)
        with self._pool.connection() as c:
            return c.execute(sql, args).rowcount

    def purge(self, now: float, retain_seconds: float, dead_retain_seconds: float | None = None):
        dead = retain_seconds if dead_retain_seconds is None else dead_retain_seconds
        self._exec(
            "DELETE FROM deliveries WHERE finished IS NOT NULL AND finished<%s AND"
            " (status<>'dead' OR finished<%s)", (now - retain_seconds, now - dead))

    def stats(self) -> dict:
        with self._pool.connection() as c:
            rows = c.execute("SELECT status, COUNT(*) AS c FROM deliveries GROUP BY status").fetchall()
        return {r["status"]: r["c"] for r in rows}

    def get(self, id_: int) -> dict | None:
        with self._pool.connection() as c:
            return c.execute("SELECT * FROM deliveries WHERE id=%s", (id_,)).fetchone()

    def _exec(self, sql: str, args: tuple):
        with self._pool.connection() as c:
            c.execute(sql, args)
