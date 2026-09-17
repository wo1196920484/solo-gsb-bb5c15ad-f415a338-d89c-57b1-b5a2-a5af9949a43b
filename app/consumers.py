"""Consumer cursors and claim/ack leases (persisted in SQLite).

A claim returns up to ``max_events`` applied, contiguous events starting at the
consumer's cursor and hands out a lease token with an ``expires_at`` deadline.
Re-claiming while the lease is still valid returns the exact same batch. An ack
carrying the current, unexpired token atomically advances the cursor; expired
or superseded tokens are rejected with 409.
"""

from __future__ import annotations

import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from . import config, db
from .repo import ConflictError


class ConsumerNotFoundError(Exception):
    """Raised when neither the stream nor the consumer exists (HTTP 404)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ConsumerEvent:
    event_id: str
    sequence: int
    delta: int

    def as_dict(self) -> dict[str, Any]:
        return {"event_id": self.event_id, "sequence": self.sequence, "delta": self.delta}


@dataclass(frozen=True)
class Claim:
    lease_token: str
    expires_at: int
    start_sequence: int
    events: list[ConsumerEvent]

    @property
    def event_count(self) -> int:
        return len(self.events)

    @property
    def next_sequence(self) -> int:
        """First sequence after the leased batch (equals the cursor on empty)."""
        return self.start_sequence + len(self.events)

    def as_dict(self) -> dict[str, Any]:
        return {
            "lease_token": self.lease_token,
            "expires_at": self.expires_at,
            "start_sequence": self.start_sequence,
            "event_count": self.event_count,
            "events": [event.as_dict() for event in self.events],
        }


@dataclass(frozen=True)
class Ack:
    stream_id: str
    consumer_id: str
    cursor_sequence: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "cursor_sequence": self.cursor_sequence,
        }


def _stream_exists(conn: sqlite3.Connection, stream_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
    ).fetchone() is not None


def _get_consumer(
    conn: sqlite3.Connection, stream_id: str, consumer_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT stream_id, consumer_id, cursor_sequence FROM consumers "
        "WHERE stream_id = ? AND consumer_id = ?",
        (stream_id, consumer_id),
    ).fetchone()


def _get_lease(
    conn: sqlite3.Connection, stream_id: str, consumer_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT lease_token, start_sequence, next_sequence, event_count, "
        "created_at, expires_at FROM consumer_leases "
        "WHERE stream_id = ? AND consumer_id = ?",
        (stream_id, consumer_id),
    ).fetchone()


def _fetch_events(
    conn: sqlite3.Connection, stream_id: str, start_sequence: int, max_events: int
) -> list[ConsumerEvent]:
    """Return up to ``max_events`` applied events from ``start_sequence`` on.

    Applied events always form the contiguous prefix [0, next_sequence), so the
    first missing row marks the end of the available prefix.
    """
    rows = conn.execute(
        "SELECT event_id, sequence, delta FROM events "
        "WHERE stream_id = ? AND sequence >= ? AND applied = 1 "
        "ORDER BY sequence ASC LIMIT ?",
        (stream_id, start_sequence, max_events),
    ).fetchall()
    events = [
        ConsumerEvent(event_id=row["event_id"], sequence=row["sequence"], delta=row["delta"])
        for row in rows
    ]
    # Defensive trimming: stop at the first non-contiguous sequence even if such
    # a row should be impossible given the catch-up merge invariants.
    contiguous: list[ConsumerEvent] = []
    expected = start_sequence
    for event in events:
        if event.sequence != expected:
            break
        contiguous.append(event)
        expected += 1
    return contiguous


def _lease_events(
    conn: sqlite3.Connection, stream_id: str, lease: sqlite3.Row
) -> list[ConsumerEvent]:
    """Re-read the exact events covered by an existing lease."""
    if lease["event_count"] == 0:
        return []
    rows = conn.execute(
        "SELECT event_id, sequence, delta FROM events "
        "WHERE stream_id = ? AND sequence >= ? AND sequence < ? AND applied = 1 "
        "ORDER BY sequence ASC",
        (stream_id, lease["start_sequence"], lease["next_sequence"]),
    ).fetchall()
    return [
        ConsumerEvent(event_id=row["event_id"], sequence=row["sequence"], delta=row["delta"])
        for row in rows
    ]


def claim(
    stream_id: str, consumer_id: str, max_events: int
) -> Claim | None:
    """Lease the next contiguous, applied event batch for a consumer.

    Returns the same :class:`Claim` while a valid lease is outstanding (the
    ``max_events`` argument is ignored in that case). Returns ``None`` when the
    stream exists but no applied event is available at the cursor.
    """
    now = int(time.time())

    # Fast path: a still-valid lease is returned straight from a read
    # connection, so repeat claims do not contend with writers. The write path
    # re-checks the lease under BEGIN IMMEDIATE, closing the read/lock race.
    with db.read_connection() as conn:
        lease = _get_lease(conn, stream_id, consumer_id)
        if lease is not None and lease["expires_at"] > now:
            return Claim(
                lease_token=lease["lease_token"],
                expires_at=lease["expires_at"],
                start_sequence=lease["start_sequence"],
                events=_lease_events(conn, stream_id, lease),
            )

    with db.write_transaction() as conn:
        if not _stream_exists(conn, stream_id):
            raise ConsumerNotFoundError(
                "stream_not_found", f"unknown stream {stream_id!r}"
            )

        consumer = _get_consumer(conn, stream_id, consumer_id)
        if consumer is None:
            conn.execute(
                "INSERT INTO consumers (stream_id, consumer_id, cursor_sequence) "
                "VALUES (?, ?, 0)",
                (stream_id, consumer_id),
            )
            cursor = 0
        else:
            cursor = consumer["cursor_sequence"]

        lease = _get_lease(conn, stream_id, consumer_id)
        if lease is not None:
            if lease["expires_at"] > now:
                # Another instance may have created the lease after our read.
                return Claim(
                    lease_token=lease["lease_token"],
                    expires_at=lease["expires_at"],
                    start_sequence=lease["start_sequence"],
                    events=_lease_events(conn, stream_id, lease),
                )
            # Expired: the same batch may be handed out again.
            conn.execute(
                "DELETE FROM consumer_leases WHERE stream_id = ? AND consumer_id = ?",
                (stream_id, consumer_id),
            )

        events = _fetch_events(conn, stream_id, cursor, max_events)
        if not events:
            return None

        token = secrets.token_urlsafe(32)
        expires_at = now + config.lease_ttl_seconds()
        conn.execute(
            "INSERT INTO consumer_leases "
            "(stream_id, consumer_id, lease_token, start_sequence, next_sequence, "
            "event_count, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stream_id,
                consumer_id,
                token,
                cursor,
                cursor + len(events),
                len(events),
                now,
                expires_at,
            ),
        )
        return Claim(
            lease_token=token,
            expires_at=expires_at,
            start_sequence=cursor,
            events=events,
        )


def ack(stream_id: str, consumer_id: str, token: str) -> Ack:
    """Atomically advance the cursor when ``token`` is the current valid lease.

    Raises :class:`ConsumerNotFoundError` for unknown stream/consumer and
    :class:`ConflictError` when the token is missing, expired or superseded
    (all mapped to HTTP 409).
    """
    now = int(time.time())
    with db.write_transaction() as conn:
        consumer = _get_consumer(conn, stream_id, consumer_id)
        if consumer is None:
            if not _stream_exists(conn, stream_id):
                raise ConsumerNotFoundError(
                    "stream_not_found", f"unknown stream {stream_id!r}"
                )
            raise ConsumerNotFoundError(
                "consumer_not_found",
                f"unknown consumer {consumer_id!r} for stream {stream_id!r}; claim first",
            )

        lease = _get_lease(conn, stream_id, consumer_id)
        if lease is None:
            raise ConflictError(
                "lease_not_found", "no outstanding lease for this consumer; claim first"
            )
        if lease["lease_token"] != token:
            raise ConflictError(
                "stale_lease_token",
                "lease token does not match the current lease",
            )
        if lease["expires_at"] <= now:
            raise ConflictError(
                "lease_expired",
                "lease has expired; the batch must be claimed again",
            )

        # Cursor only moves forward: the leased start equals the cursor captured
        # when the lease was granted.
        new_cursor = max(consumer["cursor_sequence"], lease["next_sequence"])
        conn.execute(
            "UPDATE consumers SET cursor_sequence = ? WHERE stream_id = ? AND consumer_id = ?",
            (new_cursor, stream_id, consumer_id),
        )
        conn.execute(
            "DELETE FROM consumer_leases WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        )
        return Ack(
            stream_id=stream_id,
            consumer_id=consumer_id,
            cursor_sequence=new_cursor,
        )
