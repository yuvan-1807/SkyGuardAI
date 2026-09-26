"""Durable store-and-forward queue for virtual/real edge transport.

Messages are removed only after the backend acknowledges them. Network failures,
5xx and 429 responses remain queued with exponential backoff.
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class EdgeOutbox:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            yield conn
        except BaseException:
            try:
                conn.rollback()
            finally:
                conn.close()
            raise
        else:
            try:
                conn.commit()
            finally:
                conn.close()

    def _init(self):
        with self._conn() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS queue(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                device_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                attempts INTEGER DEFAULT 0,
                next_attempt REAL DEFAULT 0,
                last_error TEXT,
                status TEXT DEFAULT 'PENDING'
            )""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_queue_due ON queue(status, next_attempt, id)")
            c.execute("""CREATE TABLE IF NOT EXISTS meta(
                device_id TEXT PRIMARY KEY,
                next_sequence INTEGER NOT NULL DEFAULT 1
            )""")

    def next_sequence(self, device_id: str) -> int:
        with self._conn() as c:
            row = c.execute("SELECT next_sequence FROM meta WHERE device_id=?", (device_id,)).fetchone()
            if row is None:
                c.execute("INSERT INTO meta(device_id,next_sequence) VALUES(?,2)", (device_id,))
                return 1
            seq = int(row[0])
            c.execute("UPDATE meta SET next_sequence=? WHERE device_id=?", (seq + 1, device_id))
            return seq

    def clear_pending_keep_sequences(self) -> None:
        """Clear demo queue rows without resetting per-device sequence counters."""
        with self._conn() as c:
            c.execute("DELETE FROM queue")

    def enqueue(self, envelope: dict[str, Any]) -> None:
        with self._conn() as c:
            c.execute("""INSERT OR IGNORE INTO queue(message_id,device_id,kind,payload_json,created_at,next_attempt,status)
                         VALUES(?,?,?,?,?,?, 'PENDING')""",
                      (envelope["message_id"], envelope["device_id"], envelope["kind"],
                       json.dumps(envelope, separators=(",", ":"), ensure_ascii=False), time.time(), 0.0))

    def pending(self, limit: int = 10):
        with self._conn() as c:
            return c.execute("""SELECT id,message_id,device_id,kind,payload_json,attempts,next_attempt
                              FROM queue WHERE status='PENDING' AND next_attempt <= ? ORDER BY id LIMIT ?""",
                             (time.time(), int(limit))).fetchall()

    def ack(self, message_id: str):
        with self._conn() as c:
            c.execute("DELETE FROM queue WHERE message_id=?", (message_id,))

    def fail(self, message_id: str, error: str, permanent: bool = False):
        with self._conn() as c:
            if permanent:
                c.execute("UPDATE queue SET status='DEAD', last_error=? WHERE message_id=?", (error[:500], message_id))
            else:
                row = c.execute("SELECT attempts FROM queue WHERE message_id=?", (message_id,)).fetchone()
                attempts = int(row[0] if row else 0) + 1
                delay = min(300.0, 2.0 ** min(attempts, 8))
                c.execute("UPDATE queue SET attempts=?, next_attempt=?, last_error=? WHERE message_id=?",
                          (attempts, time.time() + delay, error[:500], message_id))

    def stats(self) -> dict[str, int]:
        with self._conn() as c:
            pending = int(c.execute("SELECT COUNT(*) FROM queue WHERE status='PENDING'").fetchone()[0] or 0)
            dead = int(c.execute("SELECT COUNT(*) FROM queue WHERE status='DEAD'").fetchone()[0] or 0)
        return {"pending": pending, "dead": dead}

    def oldest_age_seconds(self) -> float | None:
        with self._conn() as c:
            row = c.execute("SELECT MIN(created_at) FROM queue WHERE status='PENDING'").fetchone()
        if not row or row[0] is None:
            return None
        return max(0.0, time.time() - float(row[0]))

    def clear_dead(self):
        with self._conn() as c:
            c.execute("DELETE FROM queue WHERE status='DEAD'")
