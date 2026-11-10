import json
import sqlite3
import threading
import time


class Store:
    """Durable delivery queue. Bodies are kept only until delivered/dead + retention."""

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
        self._lock = threading.Lock()

    def enqueue(self, provider, route, url, mode, body: bytes, headers: dict, now: float | None = None) -> int:
        now = now if now is not None else time.time()
        with self._lock, self._db:
            cur = self._db.execute(
                "INSERT INTO deliveries(provider,route,url,mode,body,headers,next_at,created) VALUES(?,?,?,?,?,?,?,?)",
                (provider, route, url, mode, body, json.dumps(headers), now, now),
            )
            return cur.lastrowid

    def due(self, now: float, limit: int = 20) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(
                "SELECT * FROM deliveries WHERE status='pending' AND next_at<=? ORDER BY id LIMIT ?", (now, limit)
            ).fetchall()

    def mark_done(self, id_: int, now: float):
        with self._lock, self._db:
            self._db.execute("UPDATE deliveries SET status='done', finished=?, body=NULL WHERE id=?", (now, id_))

    def mark_retry(self, id_: int, attempts: int, next_at: float, error: str):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE deliveries SET attempts=?, next_at=?, last_error=? WHERE id=?", (attempts, next_at, error, id_)
            )

    def mark_dead(self, id_: int, attempts: int, now: float, error: str):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE deliveries SET status='dead', attempts=?, finished=?, last_error=? WHERE id=?",
                (attempts, now, error, id_),
            )

    def purge(self, now: float, retain_seconds: float):
        with self._lock, self._db:
            self._db.execute("DELETE FROM deliveries WHERE finished IS NOT NULL AND finished<?", (now - retain_seconds,))

    def stats(self) -> dict:
        with self._lock:
            rows = self._db.execute("SELECT status, COUNT(*) c FROM deliveries GROUP BY status").fetchall()
        return {r["status"]: r["c"] for r in rows}
