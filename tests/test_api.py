"""End-to-end API tests (requires fastapi + httpx, installed via requirements-dev.txt)."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    db_dir = tmp_path_factory.mktemp("db")
    os.environ["DATABASE_PATH"] = str(db_dir / "test_events.db")

    from fastapi.testclient import TestClient  # noqa: E402

    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c


def _post(client, stream_id, event_id, sequence, delta):
    return client.post(
        f"/streams/{stream_id}/events",
        json={"event_id": event_id, "sequence": sequence, "delta": delta},
    )


def test_in_order_events_accumulate(client):
    r = _post(client, "s-ordered", "e0", 0, 100)
    assert r.status_code == 201
    assert r.json()["stream"] == {
        "stream_id": "s-ordered",
        "balance": 100,
        "next_sequence": 1,
        "pending_sequences": [],
    }

    r = _post(client, "s-ordered", "e1", 1, -30)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 70
    assert r.json()["stream"]["next_sequence"] == 2


def test_gap_is_buffered_then_caught_up_atomically(client):
    # Sequence 1 arrives before sequence 0: nothing is applied yet.
    r = _post(client, "s-gap", "e1", 1, 200)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 0
    assert body["stream"]["next_sequence"] == 0
    assert body["stream"]["pending_sequences"] == [1]

    r = client.get("/streams/s-gap")
    assert r.json()["pending_sequences"] == [1]

    # Missing event arrives: both events are applied in the same transaction.
    r = _post(client, "s-gap", "e0", 0, 50)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 250
    assert body["stream"]["next_sequence"] == 2
    assert body["stream"]["pending_sequences"] == []


def test_replay_is_idempotent(client):
    _post(client, "s-rep", "e0", 0, 10)

    r = _post(client, "s-rep", "e0", 0, 10)
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert r.json()["stream"]["balance"] == 10
    assert r.json()["stream"]["next_sequence"] == 1


def test_replay_with_different_content_returns_409(client):
    _post(client, "s-conf", "e0", 0, 10)

    r = _post(client, "s-conf", "e0", 1, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "replay_conflict"

    r = _post(client, "s-conf", "e0", 0, 999)
    assert r.status_code == 409


def test_duplicate_sequence_different_event_id_returns_409(client):
    _post(client, "s-dup", "e0", 0, 10)

    r = _post(client, "s-dup", "other-event", 0, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "sequence_conflict"
    # The conflicting event was never stored.
    assert client.get("/streams/s-dup").json()["next_sequence"] == 1


def test_get_unknown_stream_returns_404(client):
    r = client.get("/streams/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_validation_rejects_bad_payloads(client):
    assert _post(client, "s-val", "", 0, 1).status_code == 422
    assert _post(client, "s-val", "e", -1, 1).status_code == 422
    assert _post(client, "s-val", "e", 0, "not-an-int").status_code == 422
    assert _post(client, "s-val", "e", True, 1).status_code == 422


def test_large_out_of_order_batch_catches_up(client):
    # Submit 1..9 (all buffered because 0 is missing), then 0.
    for seq in range(1, 10):
        r = _post(client, "s-batch", f"e{seq}", seq, seq)
        assert r.json()["stream"]["next_sequence"] == 0
    assert client.get("/streams/s-batch").json()["pending_sequences"] == list(range(1, 10))

    r = _post(client, "s-batch", "e0", 0, 0)
    body = r.json()
    assert body["stream"]["next_sequence"] == 10
    assert body["stream"]["balance"] == sum(range(10))
    assert body["stream"]["pending_sequences"] == []


def test_concurrent_submissions_apply_each_sequence_once(client):
    import threading

    def post(seq):
        _post(client, "s-conc", f"e{seq}", seq, 1)

    threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = client.get("/streams/s-conc").json()
    assert state["next_sequence"] == 20
    assert state["balance"] == 20
    assert state["pending_sequences"] == []
