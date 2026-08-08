"""FastAPI entrypoint."""

from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, Response, status
from sqlalchemy import text

from app.config import get_settings
from app.db import get_engine

app = FastAPI(title="DigitFlow")


@app.get("/api/health")
async def health(response: Response) -> dict[str, Any]:
    """Report connectivity to Postgres and Redis.

    Returns 503 when any dependency is unreachable: the container healthcheck
    consumes this endpoint and can only tell states apart by status code.
    """
    settings = get_settings()
    checks: dict[str, str] = {}

    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 — report the state, do not fail the endpoint
        checks["postgres"] = f"error: {exc}"

    redis = aioredis.from_url(settings.redis_url)
    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"
    finally:
        await redis.aclose()

    healthy = set(checks.values()) == {"ok"}
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ok" if healthy else "degraded", "checks": checks}
