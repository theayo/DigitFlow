"""Celery task wrapping the download run.

The task body is a thin adapter: Celery is synchronous, the run is not. Each
invocation gets its own event loop, so the cached engine is disposed and its
cache cleared afterwards — an engine outliving its loop is unusable.

Diagnostics have a single owner at any moment. Until the runner starts, failures
belong to this module and are recorded here. Once `DownloadRunner.execute()` is
entered, the run reports its own outcome, and nothing here may overwrite it.
"""

import asyncio
import contextlib
from contextlib import AsyncExitStack

import httpx
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.config import get_settings
from app.db import get_engine, get_sessionmaker
from app.models import DownloadRun, RunEvent
from app.time import utc_now
from app.worker.celery_app import celery_app
from app.worker.client import FilesApiClient
from app.worker.downloader import DownloadRunner
from app.worker.errors import LockLost
from app.worker.lock import RunLock
from app.worker.ratelimit import RateLimiter


async def execute_run(run_id: int) -> str:
    """Set up the dependencies for one run and execute it."""
    settings = get_settings()
    redis = aioredis.from_url(settings.redis_url)

    try:
        lock = RunLock(
            redis,
            run_id,
            ttl_s=settings.run_lock_ttl_s,
            heartbeat_s=settings.run_lock_heartbeat_s,
        )
        return await _run_with_lock(run_id, settings, lock, redis)
    finally:
        await redis.aclose()
        await get_engine().dispose()
        get_engine.cache_clear()
        get_sessionmaker.cache_clear()


async def _run_with_lock(
    run_id: int,
    settings,
    lock: RunLock,
    redis: aioredis.Redis,
) -> str:
    async with AsyncExitStack() as stack:
        # The guard covers preparation only. Widening it over execute() would let a
        # mid-download Redis failure — already recorded by the runner — be rewritten
        # as a preparation error, together with a duplicate log entry.
        try:
            acquired = await stack.enter_async_context(lock.hold())
            if not acquired:
                # The slot is genuinely taken. The user already got a 409 from
                # POST /api/runs; retrying here would only fight the run that
                # legitimately holds it.
                await _finish_early(run_id, f"уже выполняется другой ран ({await _holder(lock)})")
                return "skipped"

            limiter = RateLimiter(
                redis,
                min_interval_ms=settings.external_min_interval_ms,
                max_interval_ms=settings.external_min_interval_max_ms,
                max_retry_wait_s=settings.max_retry_wait_s,
            )
            await limiter.reset()

            http = await stack.enter_async_context(
                httpx.AsyncClient(
                    base_url=settings.external_api_base_url,
                    timeout=settings.external_timeout_s,
                )
            )
            runner = DownloadRunner(
                run_id=run_id,
                settings=settings,
                client=FilesApiClient(settings, limiter, http),
                lock=lock,
                sessionmaker=get_sessionmaker(),
                redis=redis,
            )
        except (LockLost, RedisError) as exc:
            # Redis is unreachable, so ownership cannot be established or verified.
            # Reported as exactly that — never disguised as a busy slot, which would
            # send whoever reads the log looking for a run that never existed.
            await _finish_early(run_id, f"Redis недоступен на подготовке рана: {exc}")
            return "failed"

        # From here the runner owns the diagnostics of this run.
        status = await runner.execute()

    # The penalty outlives long pauses on purpose, so a finished run has to clear
    # it explicitly. Its TTL only covers a crash, and a cleanup failure must not
    # turn a completed run into a failed one.
    with contextlib.suppress(RedisError):
        await limiter.reset()
    return status


async def _holder(lock: RunLock) -> str:
    """Name the run holding the slot, without letting Redis break the report."""
    try:
        return str(await lock.holder())
    except RedisError as exc:
        return f"владелец неизвестен, Redis недоступен: {exc}"


async def _finish_early(run_id: int, reason: str) -> None:
    """Close a run that never started working.

    Writes only to PostgreSQL, so it still records the outcome when Redis is the
    very thing that failed.

    Acts on a `pending` run only. A run that already reached `running` — or any
    terminal state — has its own recorded outcome, and overwriting it would
    replace the real cause with this one and duplicate the log entry.
    """
    async with get_sessionmaker()() as session:
        run = await session.get(DownloadRun, run_id)
        if run is None or run.status != "pending":
            return
        run.status = "failed"
        run.finished_at = utc_now()
        run.error = reason
        session.add(
            RunEvent(
                run_id=run_id,
                level="error",
                message=f"запуск прерван: {reason}",
                ts=utc_now(),
            )
        )
        await session.commit()


@celery_app.task(name="download.run")
def run_download(run_id: int) -> str:
    return asyncio.run(execute_run(run_id))
