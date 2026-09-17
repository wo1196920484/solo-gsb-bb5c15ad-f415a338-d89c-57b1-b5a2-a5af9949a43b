"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    db_dir = tmp_path_factory.mktemp("db")
    os.environ["DATABASE_PATH"] = str(db_dir / "test_events.db")

    from fastapi.testclient import TestClient  # noqa: E402

    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c
