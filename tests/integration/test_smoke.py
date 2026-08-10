"""Smoke tests for the skeleton: health endpoint and database constraints.

Nothing here talks to the external API — no layer that does exists yet.
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import IntegrityError

from app.db import get_sessionmaker
from app.main import app
from app.models import CONTENT_LENGTH, File


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_health_ok() -> None:
    """All dependencies reachable: 200 and status=ok."""
    async with _client() as client:
        response = await client.get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["checks"] == {"postgres": "ok", "redis": "ok"}


async def test_health_degraded_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreachable dependency must yield 503, otherwise the healthcheck sees nothing."""

    def broken_engine() -> None:
        raise RuntimeError("postgres is unreachable")

    monkeypatch.setattr("app.main.get_engine", broken_engine)

    async with _client() as client:
        response = await client.get("/api/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"].startswith("error:")


async def test_negative_digit_count_rejected() -> None:
    """The database must reject a negative counter.

    The sum is deliberately 500 so that the range constraint is exercised
    rather than the sum constraint.
    """
    counts = {f"d{i}": 0 for i in range(10)}
    counts["d0"] = -1
    counts["d1"] = 501

    async with get_sessionmaker()() as session:
        session.add(
            File(
                name=f"{uuid.uuid4()}.txt",
                content="0" * CONTENT_LENGTH,
                content_hash="0" * 64,
                **counts,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()
