"""Preparation-stage failures of the Celery task.

A run must never be left in `pending`: that state reads as work in progress and
blocks the UI from offering a new start. None of these tests reach the external
API — every path fails before the HTTP client is even created.
"""

import uuid

import pytest
import redis.asyncio as aioredis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import DownloadRun, RunEvent
from app.worker import tasks
from app.worker.downloader import DownloadRunner


class FakeRedis:
    """Redis stub that fails exactly where a test needs it to."""

    def __init__(
        self,
        *,
        eval_result: int = 1,
        fail_eval: bool = False,
        fail_get: bool = False,
        fail_delete: bool = False,
    ) -> None:
        self._eval_result = eval_result
        self._fail_eval = fail_eval
        self._fail_get = fail_get
        self._fail_delete = fail_delete
        self.closed = False

    async def eval(self, *args: object) -> int:
        if self._fail_eval:
            raise RedisError("соединение с Redis потеряно")
        return self._eval_result

    async def get(self, *args: object) -> bytes:
        if self._fail_get:
            raise RedisError("соединение с Redis потеряно")
        return b"run:999"

    async def delete(self, *args: object) -> int:
        if self._fail_delete:
            raise RedisError("соединение с Redis потеряно")
        return 0

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def broken_redis(monkeypatch: pytest.MonkeyPatch):
    """Replace the Redis client the task builds for itself."""

    def install(fake: FakeRedis) -> FakeRedis:
        monkeypatch.setattr(aioredis, "from_url", lambda *args, **kwargs: fake)
        return fake

    return install


async def make_run(sessionmaker: async_sessionmaker[AsyncSession]) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(candidate_id=f"test-{uuid.uuid4()}", status="pending")
        session.add(run)
        await session.commit()
        return run.id


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


async def error_events(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> list[str]:
    async with sessionmaker() as session:
        rows = await session.execute(
            select(RunEvent.message)
            .where(RunEvent.run_id == run_id, RunEvent.level == "error")
            .order_by(RunEvent.id)
        )
        return list(rows.scalars())


async def assert_closed_run(
    sessionmaker: async_sessionmaker[AsyncSession], run_id: int
) -> DownloadRun:
    """Whatever went wrong, the run must not stay pending."""
    run = await fetch_run(sessionmaker, run_id)

    assert run.status == "failed"
    assert run.finished_at is not None
    assert run.error is not None
    assert await error_events(sessionmaker, run_id)
    return run


async def test_redis_failure_during_acquire_closes_the_run(sessionmaker, broken_redis) -> None:
    """Ownership cannot be established: reported as a Redis failure, not a busy slot."""
    broken_redis(FakeRedis(fail_eval=True))
    run_id = await make_run(sessionmaker)

    status = await tasks.execute_run(run_id)

    assert status == "failed"
    run = await assert_closed_run(sessionmaker, run_id)
    assert "Redis недоступен" in run.error
    assert "захвате lock" in run.error
    # The misleading explanation the old code would have produced.
    assert "уже выполняется другой ран" not in run.error


async def test_busy_slot_survives_redis_failure_while_naming_the_holder(
    sessionmaker, broken_redis
) -> None:
    """The slot really is taken; only the holder's name is unavailable."""
    broken_redis(FakeRedis(eval_result=0, fail_get=True))
    run_id = await make_run(sessionmaker)

    status = await tasks.execute_run(run_id)

    assert status == "skipped"
    run = await assert_closed_run(sessionmaker, run_id)
    assert "уже выполняется другой ран" in run.error
    assert "владелец неизвестен" in run.error


async def test_busy_slot_names_the_holder(sessionmaker, broken_redis) -> None:
    broken_redis(FakeRedis(eval_result=0))
    run_id = await make_run(sessionmaker)

    status = await tasks.execute_run(run_id)

    assert status == "skipped"
    run = await assert_closed_run(sessionmaker, run_id)
    assert "run:999" in run.error


async def test_redis_failure_during_limiter_reset_closes_the_run(
    sessionmaker, broken_redis
) -> None:
    """The lock was taken, but pacing state could not be cleared."""
    broken_redis(FakeRedis(eval_result=1, fail_delete=True))
    run_id = await make_run(sessionmaker)

    status = await tasks.execute_run(run_id)

    assert status == "failed"
    run = await assert_closed_run(sessionmaker, run_id)
    assert "Redis недоступен на подготовке рана" in run.error


async def test_failure_after_start_keeps_the_runner_diagnostics(
    redis, sessionmaker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Redis failure during downloading must not be relabelled as a preparation error.

    The runner has already recorded the real cause and its own log entry by the
    time the exception reaches the task.
    """
    run_id = await make_run(sessionmaker)

    async def broken_loop(self: DownloadRunner) -> None:
        # Raised after _begin() has moved the run to running, and before any
        # request goes out: the external API is never contacted.
        raise RedisError("Redis отвалился посреди выкачки")

    monkeypatch.setattr(DownloadRunner, "_loop", broken_loop)

    with pytest.raises(RedisError):
        await tasks.execute_run(run_id)

    run = await assert_closed_run(sessionmaker, run_id)
    assert "непредвиденная ошибка" in run.error
    assert "Redis отвалился посреди выкачки" in run.error
    assert "Redis недоступен на подготовке рана" not in run.error
    assert "запуск прерван" not in run.error

    # One error event, from the runner. _finish_early must not have added a second.
    assert len(await error_events(sessionmaker, run_id)) == 1


async def test_finish_early_leaves_a_running_run_alone(sessionmaker) -> None:
    """Second line of defence: only a pending run may be closed from here."""
    run_id = await make_run(sessionmaker)
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        run.status = "running"
        await session.commit()

    await tasks._finish_early(run_id, "Redis недоступен на подготовке рана: выдумка")

    run = await fetch_run(sessionmaker, run_id)
    assert run.status == "running"
    assert run.error is None
    assert run.finished_at is None
    assert await error_events(sessionmaker, run_id) == []


async def test_redis_client_is_always_closed(sessionmaker, broken_redis) -> None:
    fake = broken_redis(FakeRedis(fail_eval=True))
    run_id = await make_run(sessionmaker)

    await tasks.execute_run(run_id)

    assert fake.closed is True
