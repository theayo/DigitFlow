"""The window between claiming a run and provably owning the slot (§ 8.8).

`starting` exists for exactly this window. A claimed run has not taken the lock
yet, so "active but lock-less" cannot mean "dead" for it — while for `running`,
which is entered only after ownership is verified, it must.

The races are driven by `asyncio.Event`, not by imitating the order by hand: the
worker is really suspended mid-startup while the reaper really runs against the
same database and the same Redis.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import DownloadRun
from app.services.runs import finish_run
from app.time import utc_now
from app.worker import tasks
from app.worker.client import NAMES_PATH
from app.worker.lock import LOCK_KEY, RedisRunLock, token_for


@dataclass
class Pause:
    """A place the worker is held at, and the switch that lets it go."""

    reached: asyncio.Event
    resume: asyncio.Event

    async def wait(self) -> None:
        await asyncio.wait_for(self.reached.wait(), timeout=10)


async def add_run(
    sessionmaker: async_sessionmaker[AsyncSession], status: str, *, age_s: float = 0.0
) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(
            candidate_id="test-startup",
            status=status,
            started_at=utc_now() - timedelta(seconds=age_s),
        )
        session.add(run)
        await session.commit()
        return run.id


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


@asynccontextmanager
async def empty_catalog() -> AsyncIterator[respx.Route]:
    """Mount the external API with an empty catalog, so a run that starts finishes.

    Every request is intercepted: a call this test does not expect cannot reach
    the live service, it fails the assertion on `call_count` instead.
    """
    async with respx.mock(
        base_url=get_settings().external_api_base_url, assert_all_called=False
    ) as mock:
        yield mock.get(NAMES_PATH).respond(200, json={"file_names": []})


def pause_before_taking_the_lock(monkeypatch: pytest.MonkeyPatch) -> Pause:
    """Suspend the worker after the claim, before the slot is taken."""
    pause = Pause(asyncio.Event(), asyncio.Event())
    original = RedisRunLock.acquire

    async def paused_acquire(self: RedisRunLock) -> bool:
        pause.reached.set()
        await pause.resume.wait()
        return await original(self)

    monkeypatch.setattr(RedisRunLock, "acquire", paused_acquire)
    return pause


def pause_before_starting_to_run(monkeypatch: pytest.MonkeyPatch) -> Pause:
    """Suspend the worker while it holds the lock but is still `starting`."""
    pause = Pause(asyncio.Event(), asyncio.Event())
    original = tasks.start_running

    async def paused_start(session: AsyncSession, run_id: int) -> bool:
        pause.reached.set()
        await pause.resume.wait()
        return await original(session, run_id)

    monkeypatch.setattr(tasks, "start_running", paused_start)
    return pause


# --- the window itself ------------------------------------------------------


async def test_a_claimed_run_holds_no_lock_yet(
    sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The premise of everything below: `starting` and lock-less is a normal state."""
    run_id = await add_run(sessionmaker, "pending")
    pause = pause_before_taking_the_lock(monkeypatch)

    async with empty_catalog():
        worker = asyncio.create_task(tasks.execute_run(run_id))
        await pause.wait()

        assert (await fetch_run(sessionmaker, run_id)).status == "starting"
        assert await redis.get(LOCK_KEY) is None

        pause.resume.set()
        assert await worker == "done"


async def test_running_appears_only_after_the_lock_is_held(
    sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ordering the reaper relies on: lock first, `running` second."""
    run_id = await add_run(sessionmaker, "pending")
    pause = pause_before_starting_to_run(monkeypatch)

    async with empty_catalog():
        worker = asyncio.create_task(tasks.execute_run(run_id))
        await pause.wait()

        # The slot is already ours, and the run still says `starting`.
        assert await redis.get(LOCK_KEY) == token_for(run_id).encode()
        assert (await fetch_run(sessionmaker, run_id)).status == "starting"

        pause.resume.set()
        assert await worker == "done"


# --- the reaper against a starting run --------------------------------------


async def test_a_fresh_starting_run_is_not_reaped(
    api, sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both reaping entry points run while the worker is mid-startup, and must not touch it."""
    run_id = await add_run(sessionmaker, "pending")
    pause = pause_before_taking_the_lock(monkeypatch)

    async with empty_catalog():
        worker = asyncio.create_task(tasks.execute_run(run_id))
        await pause.wait()

        # The start endpoint reaps and then checks the slot: the run is alive, so
        # this has to be a refusal rather than a takeover.
        assert (await api.post("/api/runs")).status_code == 409

        # And the polling endpoint reaps too, on every single poll.
        current = (await api.get("/api/runs/current")).json()
        assert current["status"] == "starting"
        assert current["active"] is True

        assert (await fetch_run(sessionmaker, run_id)).status == "starting"

        pause.resume.set()
        assert await worker == "done"

    assert (await fetch_run(sessionmaker, run_id)).status == "done"


async def test_a_stalled_starting_run_is_reaped(api, sessionmaker, redis, clean_db) -> None:
    """Past the startup grace with no lock: the worker died on its way to the slot."""
    run_id = await add_run(sessionmaker, "starting", age_s=get_settings().run_starting_grace_s + 5)

    body = (await api.get("/api/runs/current")).json()

    assert body["status"] == "failed"
    assert body["active"] is False
    run = await fetch_run(sessionmaker, run_id)
    assert "не смог начать работу" in run.error


async def test_a_starting_run_holding_the_lock_is_never_reaped(
    api, sessionmaker, redis, clean_db
) -> None:
    """Slow startup is not death: ownership outranks the grace period."""
    run_id = await add_run(sessionmaker, "starting", age_s=get_settings().run_starting_grace_s + 5)
    await redis.set(LOCK_KEY, token_for(run_id))

    body = (await api.get("/api/runs/current")).json()

    assert body["status"] == "starting"
    assert body["active"] is True


# --- losing the run while starting ------------------------------------------


async def test_a_run_closed_while_starting_downloads_nothing(
    sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The `starting` -> `running` transition fails, so the worker gives the slot back.

    This is the race the state machine exists for: whoever closed the run wins,
    and the worker must not perform a single request under a terminal run.
    """
    run_id = await add_run(sessionmaker, "pending")
    pause = pause_before_starting_to_run(monkeypatch)

    async with empty_catalog() as names:
        worker = asyncio.create_task(tasks.execute_run(run_id))
        await pause.wait()

        # Whatever the reason — an expired startup grace, an operator — the run is
        # closed while the worker is between the lock and the transition.
        async with sessionmaker() as session:
            assert await finish_run(
                session,
                run_id,
                status="failed",
                error="закрыт, пока шёл запуск",
                expected=("starting",),
            )

        pause.resume.set()
        assert await worker == "ignored"
        assert names.call_count == 0, "исполнитель под закрытым раном ходил во внешнее API"

    run = await fetch_run(sessionmaker, run_id)
    assert run.status == "failed"
    assert run.error == "закрыт, пока шёл запуск"
    # The slot is free again: the loser released what it held.
    assert await redis.get(LOCK_KEY) is None


async def test_the_startup_pause_is_a_real_suspension(
    sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guard for the tests above: without `resume` the worker really does not proceed."""
    run_id = await add_run(sessionmaker, "pending")
    pause = pause_before_taking_the_lock(monkeypatch)

    async with empty_catalog() as names:
        worker = asyncio.create_task(tasks.execute_run(run_id))
        await pause.wait()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(worker), timeout=0.2)
        assert names.call_count == 0

        pause.resume.set()
        assert await worker == "done"
