"""Integration tests for the download loop against a mocked external API.

Nothing here touches the real service: every route is served by respx.
"""

import asyncio
import io
import uuid
import zipfile
from collections.abc import Sequence

import httpx
import pytest
import redis.asyncio as aioredis
import respx
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import DownloadRun, File, RunEvent
from app.time import utc_now
from app.worker.archive import CONTENT_LENGTH, content_hash, digit_counts
from app.worker.client import DOWNLOAD_PATH, DOWNLOADED_PATH, NAMES_PATH, FilesApiClient
from app.worker.downloader import DownloadRunner
from app.worker.errors import NetworkExhausted
from app.worker.lock import LOCK_KEY, RunLock, token_for
from app.worker.progress import read as read_progress

BASE_URL = "http://external.test"


def digits(seed: int) -> str:
    return str(seed % 10) * CONTENT_LENGTH


def zip_for(names: Sequence[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for index, name in enumerate(names):
            archive.writestr(name, digits(index))
    return buffer.getvalue()


def tuned_settings():
    """Settings tuned so tests never actually sleep."""
    return get_settings().model_copy(
        update={
            "external_api_base_url": BASE_URL,
            "network_backoff_base_s": 0.0,
            "network_max_attempts": 2,
            "single_404_attempts": 2,
            "max_stale_iterations": 2,
            "external_min_interval_ms": 0,
            "external_min_interval_max_ms": 0,
        }
    )


class InstantLimiter:
    """No pacing: the pacing logic has its own tests."""

    async def reserve(self) -> float:
        return 0.0

    async def penalize(self) -> int:
        return 0


async def make_run(sessionmaker: async_sessionmaker[AsyncSession]) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(candidate_id=f"test-{uuid.uuid4()}", status="pending")
        session.add(run)
        await session.commit()
        return run.id


async def run_loop(
    run_id: int,
    redis: aioredis.Redis,
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    lock: RunLock | None = None,
    runner_cls: type[DownloadRunner] = DownloadRunner,
) -> str:
    settings = tuned_settings()
    if lock is None:
        lock = RunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
    await lock.acquire()
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        runner = runner_cls(
            run_id=run_id,
            settings=settings,
            client=FilesApiClient(settings, InstantLimiter(), http),
            lock=lock,
            sessionmaker=sessionmaker,
            redis=redis,
        )
        return await runner.execute()


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


async def saved_names(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> set[str]:
    async with sessionmaker() as session:
        rows = await session.execute(select(File.name).where(File.run_id == run_id))
        return set(rows.scalars())


async def events(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> list[str]:
    async with sessionmaker() as session:
        rows = await session.execute(
            select(RunEvent.message).where(RunEvent.run_id == run_id).order_by(RunEvent.id)
        )
        return list(rows.scalars())


# --- happy path -------------------------------------------------------------


async def test_full_catalog_is_downloaded(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)
    names = [f"{uuid.uuid4()}.txt" for _ in range(4)]

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(200, json={"file_names": names}),
                httpx.Response(200, json={"file_names": []}),
            ]
        )
        mock.post(DOWNLOAD_PATH).mock(
            side_effect=lambda request: httpx.Response(
                200, content=zip_for(request.read() and _requested(request))
            )
        )
        marked = mock.post(DOWNLOADED_PATH).mock(
            side_effect=lambda request: httpx.Response(
                200, json={"marked_now": len(_requested(request)), "already_marked": 0}
            )
        )

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "done"
    assert await saved_names(sessionmaker, run_id) == set(names)

    run = await fetch_run(sessionmaker, run_id)
    assert run.status == "done"
    assert run.names_seen == 4
    assert run.files_saved == 4
    assert run.error is None

    # 4 names against a limit of 3 per request means two download calls.
    assert marked.call_count == 2

    progress = await read_progress(redis)
    assert progress is not None
    assert progress["status"] == "done"
    assert progress["files_saved"] == "4"


def _requested(request: httpx.Request) -> list[str]:
    import json

    return json.loads(request.content)["file_names"]


async def test_digit_counts_are_stored(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(200, json={"file_names": [name]}),
                httpx.Response(200, json={"file_names": []}),
            ]
        )
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        mock.post(DOWNLOADED_PATH).respond(200, json={"marked_now": 1, "already_marked": 0})

        await run_loop(run_id, redis, sessionmaker)

    async with sessionmaker() as session:
        stored = (await session.execute(select(File).where(File.name == name))).scalar_one()

    assert stored.d0 == CONTENT_LENGTH
    assert stored.content_hash == content_hash(digits(0))
    assert sum(digit_counts(stored.content.rstrip())) == CONTENT_LENGTH


# --- pauses -----------------------------------------------------------------


async def test_429_is_waited_out_and_reported(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0"}),
                httpx.Response(200, json={"file_names": [name]}),
                httpx.Response(200, json={"file_names": []}),
            ]
        )
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        mock.post(DOWNLOADED_PATH).respond(200, json={"marked_now": 1, "already_marked": 0})

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "done"
    log = await events(sessionmaker, run_id)
    assert any("429" in message for message in log)


async def test_pause_longer_than_the_limit_fails_the_run(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(403, headers={"Retry-After": "99999"})

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "превышает предел" in run.error


# --- 404 --------------------------------------------------------------------


async def test_404_is_isolated_and_fails_the_run(redis, sessionmaker) -> None:
    """The bad name must be located, the good ones saved, and the run stopped."""
    run_id = await make_run(sessionmaker)
    good = [f"{uuid.uuid4()}.txt" for _ in range(2)]
    bad = f"{uuid.uuid4()}.txt"
    names = [*good, bad]

    def download(request: httpx.Request) -> httpx.Response:
        requested = _requested(request)
        if bad in requested:
            return httpx.Response(404, json={"detail": "нет файла"})
        return httpx.Response(200, content=zip_for(requested))

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": names})
        mock.post(DOWNLOAD_PATH).mock(side_effect=download)
        mock.post(DOWNLOADED_PATH).mock(
            side_effect=lambda request: httpx.Response(
                200, json={"marked_now": len(_requested(request)), "already_marked": 0}
            )
        )

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert bad in run.error
    # The healthy names from the split chunk were still saved.
    assert await saved_names(sessionmaker, run_id) == set(good)

    log = await events(sessionmaker, run_id)
    assert any("разбиваю на одиночные запросы" in message for message in log)


# --- conflicts --------------------------------------------------------------


async def test_diverging_content_fails_the_run(redis, sessionmaker) -> None:
    """An existing row with a different hash must not be confirmed."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"
    other = "1" * CONTENT_LENGTH

    async with sessionmaker() as session:
        session.add(
            File(
                name=name,
                content=other,
                content_hash=content_hash(other),
                downloaded_at=utc_now(),
                **{f"d{i}": digit_counts(other)[i] for i in range(10)},
            )
        )
        await session.commit()

    # assert_all_called is off on purpose: /downloaded must stay untouched.
    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": [name]})
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        confirm = mock.post(DOWNLOADED_PATH).respond(
            200, json={"marked_now": 1, "already_marked": 0}
        )

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "разошлось" in run.error
    # The invariant that matters: nothing was confirmed to the external API.
    assert confirm.call_count == 0


async def test_matching_content_is_confirmed(redis, sessionmaker) -> None:
    """Re-downloading an identical file is not an error."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"
    same = digits(0)

    async with sessionmaker() as session:
        session.add(
            File(
                name=name,
                content=same,
                content_hash=content_hash(same),
                downloaded_at=utc_now(),
                **{f"d{i}": digit_counts(same)[i] for i in range(10)},
            )
        )
        await session.commit()

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(200, json={"file_names": [name]}),
                httpx.Response(200, json={"file_names": []}),
            ]
        )
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        confirm = mock.post(DOWNLOADED_PATH).respond(
            200, json={"marked_now": 1, "already_marked": 0}
        )

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "done"
    assert confirm.call_count == 1


# --- stale guard ------------------------------------------------------------


async def test_run_without_progress_is_stopped(redis, sessionmaker) -> None:
    """Names keep arriving but nothing is ever newly marked: that is a loop."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": [name]})
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        mock.post(DOWNLOADED_PATH).respond(200, json={"marked_now": 0, "already_marked": 1})

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "без единого подтверждённого" in run.error


# --- lock -------------------------------------------------------------------


async def test_second_run_does_not_start_while_one_holds_the_lock(redis, sessionmaker) -> None:
    first = RunLock(redis, run_id=1234, ttl_s=30, heartbeat_s=10)
    assert await first.acquire() is True

    try:
        second = RunLock(redis, run_id=5678, ttl_s=30, heartbeat_s=10)
        async with second.hold() as acquired:
            assert acquired is False
    finally:
        await first.release()


# --- empty catalog ----------------------------------------------------------


async def test_empty_catalog_finishes_immediately(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)

    async with respx.mock(base_url=BASE_URL) as mock:
        names = mock.get(NAMES_PATH).respond(200, json={"file_names": []})

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "done"
    assert names.call_count == 1
    async with sessionmaker() as session:
        total = await session.scalar(
            select(func.count()).select_from(File).where(File.run_id == run_id)
        )
    assert total == 0


# --- corrupted statistics ---------------------------------------------------


async def test_wrong_digit_distribution_fails_the_run(redis, sessionmaker) -> None:
    """Content and hash agree, the counters sum to 500, but they describe another file."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"
    same = digits(0)

    wrong = [0] * 10
    wrong[0] = CONTENT_LENGTH - 1
    wrong[1] = 1

    async with sessionmaker() as session:
        session.add(
            File(
                name=name,
                content=same,
                content_hash=content_hash(same),
                downloaded_at=utc_now(),
                **{f"d{i}": wrong[i] for i in range(10)},
            )
        )
        await session.commit()

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": [name]})
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        confirm = mock.post(DOWNLOADED_PATH).respond(
            200, json={"marked_now": 1, "already_marked": 0}
        )

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "статистика цифр" in run.error
    assert confirm.call_count == 0


# --- losing the lock --------------------------------------------------------


async def test_heartbeat_losing_redis_stops_the_run(redis, sessionmaker, monkeypatch) -> None:
    """A Redis failure in the background heartbeat must terminate the run."""
    run_id = await make_run(sessionmaker)
    lock = RunLock(redis, run_id, ttl_s=30, heartbeat_s=0.05)

    async def broken_extend() -> bool:
        raise RedisError("соединение потеряно")

    monkeypatch.setattr(lock, "_extend", broken_extend)

    class SlowStart(DownloadRunner):
        """Gives the heartbeat time to fail before the first outgoing request."""

        async def _begin(self) -> None:
            await super()._begin()
            await asyncio.sleep(0.2)

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        names = mock.get(NAMES_PATH).respond(200, json={"file_names": ["a.txt"]})

        async with lock.hold() as acquired:
            assert acquired
            status = await run_loop(run_id, redis, sessionmaker, lock=lock, runner_cls=SlowStart)

    assert status == "failed"
    assert names.call_count == 0
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "Redis недоступен" in run.error

    log = await events(sessionmaker, run_id)
    assert any("Redis недоступен" in message for message in log)


async def test_lock_stolen_during_retry_after_stops_the_retry(redis, sessionmaker) -> None:
    """After the pause the request must not be repeated by a run that lost the slot."""
    run_id = await make_run(sessionmaker)
    lock = RunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
    await lock.acquire()

    def steal_then_throttle(request: httpx.Request) -> httpx.Response:
        # Someone else takes the slot while we are told to back off.
        asyncio.get_running_loop().create_task(redis.set(LOCK_KEY, token_for(999999)))
        return httpx.Response(429, headers={"Retry-After": "0"})

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        names = mock.get(NAMES_PATH).mock(side_effect=steal_then_throttle)

        status = await run_loop(run_id, redis, sessionmaker, lock=lock)

    assert status == "failed"
    # The 429 was seen once; the retry after the pause never went out.
    assert names.call_count == 1
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "перешёл другому владельцу" in run.error


async def test_lock_lost_after_commit_prevents_confirmation(redis, sessionmaker) -> None:
    """Files may stay in our database, but nothing is confirmed to the external API."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"
    lock = RunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
    await lock.acquire()

    class StealAfterCommit(DownloadRunner):
        async def _save(self, parsed):
            saved = await super()._save(parsed)
            # Exactly the window the invariant is about: committed, not yet confirmed.
            await redis.set(LOCK_KEY, token_for(888888))
            return saved

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": [name]})
        mock.post(DOWNLOAD_PATH).respond(200, content=zip_for([name]))
        confirm = mock.post(DOWNLOADED_PATH).respond(
            200, json={"marked_now": 1, "already_marked": 0}
        )

        status = await run_loop(run_id, redis, sessionmaker, lock=lock, runner_cls=StealAfterCommit)

    assert status == "failed"
    assert confirm.call_count == 0
    assert await saved_names(sessionmaker, run_id) == {name}
    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None
    assert "перешёл другому владельцу" in run.error


@pytest.mark.parametrize("bad_status", [500, 503])
async def test_persistent_5xx_fails_the_run(redis, sessionmaker, bad_status: int) -> None:
    run_id = await make_run(sessionmaker)

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(bad_status)

        with pytest.raises(NetworkExhausted):
            await run_loop(run_id, redis, sessionmaker)

    run = await fetch_run(sessionmaker, run_id)
    assert run.status == "failed"
