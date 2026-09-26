"""SQLite connection lifecycle helpers for SkyGuard AI.

`sqlite3.Connection`'s context manager manages transactions, but it does not
close the connection when the block exits.  On Windows, especially with
Python 3.14, a still-live connection can keep a temporary SQLite database
locked and cause WinError 32 during test cleanup.

This helper preserves normal sqlite transaction semantics while guaranteeing
that the connection is closed on every exit path.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator, Any


@contextmanager
def db_connect(database: Any, **kwargs: Any) -> Iterator[sqlite3.Connection]:
    """Open SQLite, commit/rollback like ``with sqlite3.connect``, then close."""
    conn = sqlite3.connect(database, **kwargs)
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
