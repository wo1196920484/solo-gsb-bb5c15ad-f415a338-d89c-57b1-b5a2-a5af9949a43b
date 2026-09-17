"""SQLite connection management and schema initialisation."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config

# Give a busy writer up to 5s to clear (WAL permits one writer + many readers).
BUSY_TIMEOUT_SECONDS = 5


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.database_path(),
        timeout=BUSY_TIMEOUT_SECONDS,
        isolation_level=None,  # autocommit mode: transactions are controlled explicitly
    )
    conn.row_factory = sqlite3.Row
    # Enforce declared types (booleans etc. must not sneak into INTEGER columns).
    conn.execute("PRAGMA strict_types = ON")
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_SECONDS * 1000}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")


def initialise() -> None:
    """Create the database file, enable WAL and create the schema."""
    db_file = Path(config.database_path())
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = _connect()
    try:
        _apply_pragmas(conn)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS streams (
                stream_id     TEXT PRIMARY KEY,
                balance       INTEGER NOT NULL DEFAULT 0,
                next_sequence INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events (
                stream_id TEXT    NOT NULL,
                event_id  TEXT    NOT NULL,
                sequence  INTEGER NOT NULL CHECK (sequence >= 0),
                delta     INTEGER NOT NULL,
                applied   INTEGER NOT NULL DEFAULT 0 CHECK (applied IN (0, 1)),
                PRIMARY KEY (stream_id, event_id),
                UNIQUE (stream_id, sequence),
                FOREIGN KEY (stream_id) REFERENCES streams(stream_id)
            );

            -- Only one event may occupy each sequence of a stream.
            CREATE INDEX IF NOT EXISTS idx_events_stream_seq
                ON events (stream_id, sequence);
            -- Fast lookup of the contiguous events that catch-up has to apply.
            CREATE INDEX IF NOT EXISTS idx_events_stream_applied_seq
                ON events (stream_id, applied, sequence);
            """
        )
    finally:
        conn.close()


@contextmanager
def write_transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection and hold an immediate write transaction until commit/rollback."""
    conn = _connect()
    try:
        _apply_pragmas(conn)
        # Acquire the write lock up front so concurrent submitters serialise
        # cleanly instead of hitting SQLITE_BUSY mid-transaction.
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


@contextmanager
def read_connection() -> Iterator[sqlite3.Connection]:
    """Open a short-lived connection for read-only queries."""
    conn = _connect()
    try:
        _apply_pragmas(conn)
        yield conn
    finally:
        conn.close()
