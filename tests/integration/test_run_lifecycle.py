"""The run state machine and the delivery guard (§ 8.8).

Celery delivers at least once and `acks_late` makes redelivery of an already
finished run ordinary; the broker can also accept a task and fail to confirm it,
leaving the API convinced it never published. Every test here is about the same
question: which of two writers reaching the same row wins, and what the loser
does instead.

The loser must not reach the external API, and that is asserted the hard way —
with respx mounted and its routes checked for zero calls. An unmocked request
would try to open a real connection, which is exactly what must never happen.
"""

import asyncio
import uuid

import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import DownloadRun, RunEvent
from app.services.runs import (
    TERMINAL_STATUSES,
    claim_run,
    finish_run,
    set_run_status,
    start_running,
)
from app.time import utc_now
from app.worker import tasks
from app.worker.client import DOWNLOAD_PATH, DOWNLOADED_PATH, NAMES_PATH
from app.worker.lock import LOCK_KEY, RedisRunLock, token_for

# Every test here builds its own run, and only one active run may exist at a time
# now that PostgreSQL enforces it (§ 7.1) — so each starts from an empty table.
pytestmark = pytest.mark.usefixtures("redis", "clean_db")


async def add_run(
    sessionmaker: async_sessionmaker[AsyncSession], status: str, **fields: object
) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(
            candidate_id=f"test-{uuid.uuid4()}",
            status=status,
            started_at=utc_now(),
            **fields,
        )
        session.add(run)
        await session.commit()
        return run.id


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


async def messages(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> list[str]:
    async with sessionmaker() as session:
        rows = await session.execute(
            select(RunEvent.message).where(RunEvent.run_id == run_id).order_by(RunEvent.id)
        )
        return list(rows.scalars())


async def execute_without_external_api(run_id: int) -> str:
    """Run the task with the external API mounted but forbidden.

    respx intercepts every outgoing request, so a call that slipped through would
    fail the assertions below rather than open a connection to the live service.
    """
    async with respx.mock(
        base_url=get_settings().external_api_base_url, assert_all_called=False
    ) as mock:
        names = mock.get(NAMES_PATH)
        download = mock.post(DOWNLOAD_PATH)
        confirm = mock.post(DOWNLOADED_PATH)

        status = await tasks.execute_run(run_id)

        assert names.call_count == 0, "исполнитель без claim ходил во внешнее API"
        assert download.call_count == 0
        assert confirm.call_count == 0
    return status


# --- the claim itself -------------------------------------------------------


async def test_only_one_of_two_executors_claims_a_pending_run(sessionmaker) -> None:
    """Two deliveries of the same run compute the same lock token: the database decides."""
    run_id = await add_run(sessionmaker, "pending")

    async def claim() -> bool:
        async with sessionmaker() as session:
            return await claim_run(session, run_id)

    outcomes = await asyncio.gather(claim(), claim())

    assert sorted(outcomes) == [False, True]
    # Claimed, not working: the slot has not been taken yet.
    assert (await fetch_run(sessionmaker, run_id)).status == "starting"


async def test_running_is_reached_only_through_starting(sessionmaker) -> None:
    """`running` means "provably holds the lock", so nothing may jump straight to it."""
    run_id = await add_run(sessionmaker, "pending")

    async with sessionmaker() as session:
        assert await start_running(session, run_id) is False
        assert await set_run_status(session, run_id, "running") is False
        assert await claim_run(session, run_id)
        # Still not enough to start working from a claimed state by the side door.
        assert await set_run_status(session, run_id, "waiting_retry") is False
        assert await start_running(session, run_id)

    assert (await fetch_run(sessionmaker, run_id)).status == "running"


async def test_a_claimed_run_is_started_only_once(sessionmaker) -> None:
    run_id = await add_run(sessionmaker, "starting")

    async def start() -> bool:
        async with sessionmaker() as session:
            return await start_running(session, run_id)

    assert sorted(await asyncio.gather(start(), start())) == [False, True]


@pytest.mark.parametrize("status", ["starting", "running", "waiting_retry", "done", "failed"])
async def test_a_run_is_claimed_only_out_of_pending(sessionmaker, status: str) -> None:
    run_id = await add_run(sessionmaker, status)

    async with sessionmaker() as session:
        assert await claim_run(session, run_id) is False

    assert (await fetch_run(sessionmaker, run_id)).status == status


@pytest.mark.parametrize("status", TERMINAL_STATUSES)
async def test_terminal_runs_are_never_revived(sessionmaker, status: str) -> None:
    """Neither a pause nor a second closing may move a settled run."""
    run_id = await add_run(sessionmaker, status, finished_at=utc_now(), error="исходная причина")

    async with sessionmaker() as session:
        assert await set_run_status(session, run_id, "waiting_retry") is False
        assert (
            await finish_run(session, run_id, status="done", error=None, files_saved=999) is False
        )

    run = await fetch_run(sessionmaker, run_id)
    assert run.status == status
    assert run.error == "исходная причина"
    assert run.files_saved == 0


async def test_finishing_writes_counters_and_the_log_entry_together(sessionmaker) -> None:
    run_id = await add_run(sessionmaker, "running")

    async with sessionmaker() as session:
        assert await finish_run(
            session,
            run_id,
            status="done",
            error=None,
            names_seen=12,
            files_saved=12,
            event="процесс завершён",
            event_level="info",
        )

    run = await fetch_run(sessionmaker, run_id)
    assert (run.status, run.names_seen, run.files_saved) == ("done", 12, 12)
    assert await messages(sessionmaker, run_id) == ["процесс завершён"]


# --- delivery ---------------------------------------------------------------


async def test_task_delivered_after_the_api_gave_up_does_nothing(sessionmaker) -> None:
    """The broker accepted the task but never confirmed it, so the API closed the run."""
    run_id = await add_run(
        sessionmaker,
        "failed",
        finished_at=utc_now(),
        error="не удалось поставить задачу в очередь: таймаут подтверждения",
    )

    status = await execute_without_external_api(run_id)

    assert status == "ignored"
    run = await fetch_run(sessionmaker, run_id)
    assert run.status == "failed"
    assert "таймаут подтверждения" in run.error


@pytest.mark.parametrize("terminal", TERMINAL_STATUSES)
async def test_redelivery_of_a_finished_run_does_nothing(sessionmaker, terminal: str) -> None:
    """acks_late means a finished run can be delivered again after a lost ack."""
    run_id = await add_run(
        sessionmaker,
        terminal,
        finished_at=utc_now(),
        names_seen=7,
        files_saved=7,
        error="исходная причина" if terminal == "failed" else None,
    )

    status = await execute_without_external_api(run_id)

    assert status == "ignored"
    run = await fetch_run(sessionmaker, run_id)
    assert run.status == terminal
    assert (run.names_seen, run.files_saved) == (7, 7)
    assert any("повторная доставка" in message for message in await messages(sessionmaker, run_id))


async def test_second_executor_leaves_the_winners_lock_alone(sessionmaker, redis) -> None:
    """The first delivery already claimed the run; the second must not join in.

    The lock cannot protect the winner here: the token is derived from the run
    id, so the second delivery computes an identical one, would be let through as
    a re-adoption, and would release the winner's slot on its way out. Standing
    down before the lock is touched is what prevents that.
    """
    run_id = await add_run(sessionmaker, "pending")
    async with sessionmaker() as session:
        assert await claim_run(session, run_id)
        assert await start_running(session, run_id)
    await redis.set(LOCK_KEY, token_for(run_id))

    status = await execute_without_external_api(run_id)

    assert status == "ignored"
    # Still running: the loser must not close a run somebody else is working on.
    assert (await fetch_run(sessionmaker, run_id)).status == "running"
    assert await redis.get(LOCK_KEY) == token_for(run_id).encode()


async def test_a_run_whose_slot_belongs_to_someone_else_stops(sessionmaker, redis) -> None:
    """The worker's own guard, now that PostgreSQL admits only one active run.

    Two racing starts can no longer produce two rows (§ 7.1), so this is what the
    Redis lock is left guarding: a slot held by something the database does not
    know about — a task published by hand, a run from before the index existed,
    or a second worker. The run is closed without a single external request.
    """
    run_id = await add_run(sessionmaker, "pending")
    stranger = RedisRunLock(redis, run_id + 10_000, ttl_s=30, heartbeat_s=10)
    assert await stranger.acquire()

    try:
        status = await execute_without_external_api(run_id)
    finally:
        await stranger.release()

    assert status == "skipped"
    closed = await fetch_run(sessionmaker, run_id)
    assert closed.status == "failed"
    assert "уже выполняется другой ран" in closed.error


async def test_a_missing_run_is_ignored_without_a_log_entry(sessionmaker) -> None:
    status = await execute_without_external_api(2_000_000_001)

    assert status == "ignored"
