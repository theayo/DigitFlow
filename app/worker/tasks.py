"""Celery task wrapping the download run — the worker's entry adapter.

Three jobs, and nothing else. It decides whether this *delivery* should run at
all, it assembles the concrete adapters (this is the worker's composition root),
and it adapts a synchronous Celery call to an asynchronous run: each invocation
gets its own event loop, so the cached engine is disposed and its cache cleared
afterwards — an engine outliving its loop is unusable.

The delivery guard comes first and is the reason this module exists in this
shape. Celery delivers at least once, and `acks_late` makes redelivery of an
already finished run entirely normal; the broker can also accept a task and fail
to confirm it, leaving the API convinced it never published. The lock cannot
sort this out — two deliveries of the same run compute the same token — so the
database claim does (§ 8.8).

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
from app.progress import RedisProgressStore
from app.services.runs import (
    SlotUnavailable,
    claim_run,
    finish_run,
    reap_orphan_runs,
    start_running,
)
from app.task_queue import DOWNLOAD_TASK_NAME
from app.time import utc_now
from app.worker.celery_app import celery_app
from app.worker.client import FilesApiClient
from app.worker.downloader import DownloadRunner
from app.worker.errors import LockLost
from app.worker.lock import RedisRunLock, RedisRunSlot
from app.worker.ratelimit import RedisRateLimiter


async def execute_run(run_id: int) -> str:
    """Set up the dependencies for one run and execute it."""
    settings = get_settings()
    redis = aioredis.from_url(settings.redis_url)

    try:
        # Before the lock, before Redis, before anything with a side effect: a
        # delivery that does not win the claim must not so much as touch the
        # lock, or it would release the winner's slot on its way out.
        if not await _claim(run_id):
            return "ignored"

        lock = RedisRunLock(
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


async def _claim(run_id: int) -> bool:
    """Take the run for this delivery, or explain in the log why we are standing down."""
    async with get_sessionmaker()() as session:
        if await claim_run(session, run_id):
            return True

        run = await session.get(DownloadRun, run_id)
        if run is None:
            return False

        # Not an error: an ambiguous or repeated delivery is expected with
        # acks_late. It is worth a line in the log all the same — otherwise a
        # task that quietly did nothing looks like a lost task.
        session.add(
            RunEvent(
                run_id=run_id,
                level="warning",
                message=(
                    f"повторная доставка задачи: ран уже в статусе {run.status}, "
                    "выполнение пропущено"
                ),
                ts=utc_now(),
            )
        )
        await session.commit()
    return False


async def _run_with_lock(
    run_id: int,
    settings,
    lock: RedisRunLock,
    redis: aioredis.Redis,
) -> str:
    async with AsyncExitStack() as stack:
        # The guard covers preparation only. Widening it over execute() would let a
        # mid-download Redis failure — already recorded by the runner — be rewritten
        # as a preparation error, together with a duplicate log entry.
        try:
            acquired = await stack.enter_async_context(lock.hold())
            if not acquired:
                # The slot is genuinely taken by a different run. The user already
                # got a 409 for the ordinary case; retrying here would only fight
                # the run that legitimately holds it.
                await _finish_early(run_id, f"уже выполняется другой ран ({await _holder(lock)})")
                return "skipped"

            # Ownership is verified before reaping, not assumed from the acquire
            # above: reaping is driven by "who owns the slot", so a lock that is
            # no longer ours would make this run reap itself and then keep working
            # under a row someone else already closed.
            await lock.ensure_owned()

            # The slot is provably ours, so any other run still marked active in
            # the database belongs to a worker that stopped working (§ 8.6). Left
            # alone it would block every future start.
            async with get_sessionmaker()() as session:
                await reap_orphan_runs(
                    session,
                    RedisRunSlot(redis),
                    pending_grace_s=settings.run_pending_grace_s,
                    starting_grace_s=settings.run_starting_grace_s,
                )

            # Only now may the run call itself running: that status is what the
            # reaper reads as "this one is supposed to hold the lock". The
            # transition is conditional, so a run closed while it was starting —
            # a stalled startup, a reaper that got there first — stops here,
            # before a single external request, and the lock goes back in the
            # `finally` of hold().
            if not await _start_working(run_id):
                return "ignored"

            limiter = RedisRateLimiter(
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
                progress=RedisProgressStore(redis),
            )
        except LockLost as exc:
            # Ownership is gone or unverifiable. Never disguised as a busy slot,
            # which would send whoever reads the log looking for a run that never
            # existed.
            await _finish_early(run_id, f"lock рана потерян на подготовке: {exc}")
            return "failed"
        except (SlotUnavailable, RedisError) as exc:
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


async def _start_working(run_id: int) -> bool:
    """Turn the claimed run into a working one, or report why we are stopping."""
    async with get_sessionmaker()() as session:
        if await start_running(session, run_id):
            return True

        # Someone closed the run while it was starting. Whoever did it recorded
        # the cause; this line only explains why nothing was downloaded under it.
        session.add(
            RunEvent(
                run_id=run_id,
                level="warning",
                message="работа не начата: ран был закрыт, пока шёл захват слота",
                ts=utc_now(),
            )
        )
        await session.commit()
    return False


async def _holder(lock: RedisRunLock) -> str:
    """Name the run holding the slot, without letting Redis break the report."""
    try:
        return str(await lock.holder())
    except RedisError as exc:
        return f"владелец неизвестен, Redis недоступен: {exc}"


async def _finish_early(run_id: int, reason: str) -> None:
    """Close a run that was claimed here but never got to work.

    Writes only to PostgreSQL, so it still records the outcome when Redis is the
    very thing that failed.

    The two states named below are exactly the ones this task can have put the
    run in during preparation: `starting` after the claim, `running` once the
    lock was verified. Both are ours alone — the claim made sure of that, and the
    runner has not been handed control yet, so there are no diagnostics of its
    own to overwrite. Anything else means somebody got there first, and the
    conditional update leaves their outcome and their log entry alone.
    """
    async with get_sessionmaker()() as session:
        await finish_run(
            session,
            run_id,
            status="failed",
            error=reason,
            expected=("starting", "running"),
            event=f"запуск прерван: {reason}",
        )


@celery_app.task(name=DOWNLOAD_TASK_NAME)
def run_download(run_id: int) -> str:
    return asyncio.run(execute_run(run_id))
