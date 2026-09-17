"""API tests for consumer claim/ack leases."""

from __future__ import annotations

import threading
import time


def _post_event(client, stream_id, event_id, sequence, delta):
    return client.post(
        f"/streams/{stream_id}/events",
        json={"event_id": event_id, "sequence": sequence, "delta": delta},
    )


def _claim(client, stream_id, consumer_id, **body):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/claim",
        json=body if body else None,
    )


def _ack(client, stream_id, consumer_id, token):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/ack",
        json={"lease_token": token},
    )


def _seed_stream(client, stream_id, n):
    for seq in range(n):
        r = _post_event(client, stream_id, f"e{seq}", seq, seq + 1)
        assert r.status_code == 201


def test_claim_unknown_stream_returns_404(client):
    r = _claim(client, "no-stream", "c1")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_claim_empty_stream_returns_empty_batch(client):
    # A stream with no events: submit an out-of-order event so the stream row
    # exists but nothing is applied.
    _post_event(client, "c-empty", "e9", 9, 1)

    r = _claim(client, "c-empty", "c1")
    assert r.status_code == 200
    body = r.json()
    assert body["lease_token"] is None
    assert body["expires_at"] is None
    assert body["event_count"] == 0
    assert body["events"] == []
    assert body["start_sequence"] == 0

    # Default body is accepted (no JSON payload required).
    assert _claim(client, "c-empty", "c1").status_code == 200


def test_claim_returns_contiguous_applied_events_with_lease(client):
    _seed_stream(client, "c-claim", 5)

    r = _claim(client, "c-claim", "c1", max_events=3)
    assert r.status_code == 200
    body = r.json()
    assert body["start_sequence"] == 0
    assert body["event_count"] == 3
    assert [e["sequence"] for e in body["events"]] == [0, 1, 2]
    assert [e["delta"] for e in body["events"]] == [1, 2, 3]
    assert [e["event_id"] for e in body["events"]] == ["e0", "e1", "e2"]
    assert isinstance(body["lease_token"], str) and len(body["lease_token"]) >= 20
    assert body["expires_at"] > int(time.time())


def test_repeat_claim_returns_same_batch_and_token(client):
    _seed_stream(client, "c-repeat", 5)

    first = _claim(client, "c-repeat", "c1", max_events=2).json()
    # Even a different max_events must not change an outstanding batch.
    second = _claim(client, "c-repeat", "c1", max_events=100).json()
    third = _claim(client, "c-repeat", "c1").json()

    for body in (second, third):
        assert body["lease_token"] == first["lease_token"]
        assert body["expires_at"] == first["expires_at"]
        assert body["event_count"] == 2
        assert [e["sequence"] for e in body["events"]] == [0, 1]


def test_ack_advances_cursor_and_next_claim_continues(client):
    _seed_stream(client, "c-ack", 5)

    batch = _claim(client, "c-ack", "c1", max_events=2).json()
    r = _ack(client, "c-ack", "c1", batch["lease_token"])
    assert r.status_code == 200
    assert r.json() == {
        "stream_id": "c-ack",
        "consumer_id": "c1",
        "cursor_sequence": 2,
    }

    nxt = _claim(client, "c-ack", "c1", max_events=10).json()
    assert nxt["start_sequence"] == 2
    assert [e["sequence"] for e in nxt["events"]] == [2, 3, 4]

    _ack(client, "c-ack", "c1", nxt["lease_token"])
    tail = _claim(client, "c-ack", "c1").json()
    assert tail["event_count"] == 0
    assert tail["lease_token"] is None
    assert tail["start_sequence"] == 5


def test_ack_wrong_token_returns_409(client):
    _seed_stream(client, "c-wrong", 3)

    mine = _claim(client, "c-wrong", "c1", max_events=1).json()
    other = _claim(client, "c-wrong", "c2", max_events=1).json()

    r = _ack(client, "c-wrong", "c1", other["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stale_lease_token"
    # Cursor must not have moved.
    again = _claim(client, "c-wrong", "c1").json()
    assert again["lease_token"] == mine["lease_token"]
    assert again["event_count"] == 1


def test_ack_after_ack_returns_409(client):
    _seed_stream(client, "c-twice", 2)
    batch = _claim(client, "c-twice", "c1").json()
    assert _ack(client, "c-twice", "c1", batch["lease_token"]).status_code == 200

    r = _ack(client, "c-twice", "c1", batch["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_found"


def test_ack_unknown_stream_or_consumer_returns_404(client):
    r = _ack(client, "no-stream", "c1", "whatever")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    _seed_stream(client, "c-noconsumer", 1)
    r = _ack(client, "c-noconsumer", "ghost", "whatever")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "consumer_not_found"


def test_claim_validation(client):
    _seed_stream(client, "c-val", 1)
    assert _claim(client, "c-val", "c1", max_events=0).status_code == 422
    assert _claim(client, "c-val", "c1", max_events=10_001).status_code == 422
    assert _claim(client, "c-val", "c1", max_events=True).status_code == 422
    assert _ack(client, "c-val", "c1", "").status_code == 422


def test_consumers_are_independent(client):
    _seed_stream(client, "c-indep", 4)

    a = _claim(client, "c-indep", "a", max_events=2).json()
    b = _claim(client, "c-indep", "b", max_events=2).json()
    assert a["lease_token"] != b["lease_token"]
    assert [e["sequence"] for e in a["events"]] == [0, 1]
    assert [e["sequence"] for e in b["events"]] == [0, 1]

    assert _ack(client, "c-indep", "a", a["lease_token"]).status_code == 200
    # b still holds its own lease and gets the same batch again.
    b_again = _claim(client, "c-indep", "b").json()
    assert b_again["lease_token"] == b["lease_token"]
    assert b_again["event_count"] == 2


def test_pending_gap_blocks_claim_at_cursor(client):
    # seq 1..4 buffered (0 missing): nothing applied, claim is empty.
    for seq in range(1, 5):
        _post_event(client, "c-gap", f"e{seq}", seq, 1)
    body = _claim(client, "c-gap", "c1").json()
    assert body["event_count"] == 0

    # Missing event arrives -> 0..4 applied and now claimable.
    _post_event(client, "c-gap", "e0", 0, 10)
    body = _claim(client, "c-gap", "c1", max_events=3).json()
    assert [e["sequence"] for e in body["events"]] == [0, 1, 2]


def test_expired_lease_redelivers_and_old_token_409(client, monkeypatch):
    monkeypatch.setenv("LEASE_TTL_SECONDS", "1")
    _seed_stream(client, "c-expiry", 3)

    first = _claim(client, "c-expiry", "c1", max_events=2).json()
    # Still valid in the meantime.
    assert _claim(client, "c-expiry", "c1").json()["lease_token"] == first["lease_token"]

    time.sleep(1.1)

    # Old token can no longer ack.
    r = _ack(client, "c-expiry", "c1", first["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"

    # Same batch is claimable again under a fresh token.
    second = _claim(client, "c-expiry", "c1", max_events=2).json()
    assert second["lease_token"] != first["lease_token"]
    assert [e["sequence"] for e in second["events"]] == [0, 1]
    assert second["expires_at"] > first["expires_at"]

    assert _ack(client, "c-expiry", "c1", second["lease_token"]).status_code == 200
    assert _claim(client, "c-expiry", "c1").json()["start_sequence"] == 2


def test_concurrent_claims_share_one_lease(client):
    _seed_stream(client, "c-conc", 20)

    tokens, errors = [], []

    def claim():
        try:
            r = _claim(client, "c-conc", "cg", max_events=5)
            tokens.append(r.json()["lease_token"])
        except Exception:  # pragma: no cover - surfaced below
            import traceback

            errors.append(traceback.format_exc())

    threads = [threading.Thread(target=claim) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(tokens) == 20
    assert len(set(tokens)) == 1
