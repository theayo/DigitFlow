"""FastAPI entrypoint."""

from typing import Any

from fastapi import FastAPI, Response, status
from sqlalchemy import text

from app.api import files, runs, stats
from app.config import get_settings
from app.db import get_engine, get_redis
from app.schemas import ServiceConfig

app = FastAPI(title="DigitFlow")

app.include_router(runs.router)
app.include_router(files.router)
app.include_router(stats.router)


@app.get("/api/config", response_model=ServiceConfig)
async def config() -> ServiceConfig:
    """What this deployment allows the UI to offer.

    The demo switch is drawn only where it is permitted; the server checks the
    same setting again when a run is started, so hiding the control is a
    convenience and not the guard.
    """
    return ServiceConfig(demo_mode=get_settings().demo_mode)


@app.get("/api/health")
async def health(response: Response) -> dict[str, Any]:
    """Report connectivity to Postgres and Redis.

    Returns 503 when any dependency is unreachable: the container healthcheck
    consumes this endpoint and can only tell states apart by status code.
    """
    checks: dict[str, str] = {}

    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 — report the state, do not fail the endpoint
        checks["postgres"] = f"error: {exc}"

    try:
        await get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"

    healthy = set(checks.values()) == {"ok"}
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ok" if healthy else "degraded", "checks": checks}
