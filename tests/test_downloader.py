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
from app.progress import RedisProgressStore
from app.services.runs import claim_run, start_running
from app.time import utc_now
from app.worker.archive import CONTENT_LENGTH, content_hash, digit_counts
from app.worker.client import DOWNLOAD_PATH, DOWNLOADED_PATH, NAMES_PATH, FilesApiClient
from app.worker.downloader import DownloadRunner, _files_word
from app.worker.errors import NetworkExhausted
from app.worker.lock import LOCK_KEY, RedisRunLock, token_for

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
    """No pacing: the pacing logic has its own tests. Implements `RateLimiter`."""

    async def reserve(self) -> float:
        return 0.0

    async def penalize(self) -> int:
        return 0

    async def reset(self) -> None:
        return None


async def make_run(sessionmaker: async_sessionmaker[AsyncSession]) -> int:
    """A run in the state the task adapter hands to the runner: claimed and running."""
    async with sessionmaker() as session:
        run = DownloadRun(candidate_id=f"test-{uuid.uuid4()}", status="pending")
        session.add(run)
        await session.commit()
        run_id = run.id
        # The runner never sets the status itself and refuses to work on anything
        # but `running`, so the whole startup path is replayed here: pending ->
        # starting on claim, starting -> running once the slot is held (§ 8.8).
        assert await claim_run(session, run_id)
        assert await start_running(session, run_id)
        return run_id


async def run_loop(
    run_id: int,
    redis: aioredis.Redis,
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    lock: RedisRunLock | None = None,
    runner_cls: type[DownloadRunner] = DownloadRunner,
) -> str:
    settings = tuned_settings()
    if lock is None:
        lock = RedisRunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
    await lock.acquire()
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        runner = runner_cls(
            run_id=run_id,
            settings=settings,
            client=FilesApiClient(settings, InstantLimiter(), http),
            lock=lock,
            sessionmaker=sessionmaker,
            progress=RedisProgressStore(redis),
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

    progress = await RedisProgressStore(redis).read()
    assert progress is not None
    assert progress.status == "done"
    assert progress.files_saved == 4


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


@pytest.mark.parametrize("bad_position", [0, 1, 2], ids=["первым", "в середине", "последним"])
async def test_isolation_does_not_depend_on_the_order_of_names(
    redis, sessionmaker, bad_position: int
) -> None:
    """A file that cannot be obtained must not take its chunk-mates down with it.

    Stopping at the first bad name made the outcome depend on where it happened
    to sit: the same chunk saved two files or none. The names arrive in a random
    order from /names, so that is a coin toss over how much of the catalog gets
    downloaded before the run stops.
    """
    run_id = await make_run(sessionmaker)
    good = [f"good-{index}-{uuid.uuid4()}.txt" for index in range(2)]
    bad = f"bad-{uuid.uuid4()}.txt"
    names = [*good]
    names.insert(bad_position, bad)

    def download(request: httpx.Request) -> httpx.Response:
        requested = _requested(request)
        if bad in requested:
            return httpx.Response(404, json={"detail": "нет файла"})
        return httpx.Response(200, content=zip_for(requested))

    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": names})
        mock.post(DOWNLOAD_PATH).mock(side_effect=download)
        confirmed: list[str] = []

        def mark(request: httpx.Request) -> httpx.Response:
            requested = _requested(request)
            confirmed.extend(requested)
            return httpx.Response(200, json={"marked_now": len(requested), "already_marked": 0})

        mock.post(DOWNLOADED_PATH).mock(side_effect=mark)

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    # Every healthy name of the chunk is saved and confirmed, wherever the bad
    # one sat.
    assert await saved_names(sessionmaker, run_id) == set(good)
    assert sorted(confirmed) == sorted(good)
    assert bad not in confirmed

    run = await fetch_run(sessionmaker, run_id)
    assert run.error is not None and bad in run.error


async def test_several_bad_names_are_all_reported(redis, sessionmaker) -> None:
    run_id = await make_run(sessionmaker)
    good = f"good-{uuid.uuid4()}.txt"
    first_bad = f"bad-a-{uuid.uuid4()}.txt"
    second_bad = f"bad-b-{uuid.uuid4()}.txt"
    names = [first_bad, good, second_bad]

    def download(request: httpx.Request) -> httpx.Response:
        requested = _requested(request)
        if first_bad in requested or second_bad in requested:
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
    assert await saved_names(sessionmaker, run_id) == {good}

    run = await fetch_run(sessionmaker, run_id)
    assert first_bad in run.error and second_bad in run.error


# --- what the log says about confirmations ----------------------------------


async def download_two_names(
    redis, sessionmaker, run_id: int, names: list[str], confirmation: dict[str, int]
) -> list[str]:
    """One iteration with a fixed answer from /downloaded. Returns the log."""
    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(200, json={"file_names": names}),
                httpx.Response(200, json={"file_names": []}),
            ]
        )
        mock.post(DOWNLOAD_PATH).mock(
            side_effect=lambda request: httpx.Response(200, content=zip_for(_requested(request)))
        )
        mock.post(DOWNLOADED_PATH).respond(200, json=confirmation)

        await run_loop(run_id, redis, sessionmaker)

    return await events(sessionmaker, run_id)


async def test_the_ordinary_confirmation_says_nothing_about_zeroes(redis, sessionmaker) -> None:
    """`already_marked = 0` is the normal case: printing it on every chunk is noise."""
    run_id = await make_run(sessionmaker)
    names = [f"{uuid.uuid4()}.txt" for _ in range(2)]

    log = await download_two_names(
        redis, sessionmaker, run_id, names, {"marked_now": 2, "already_marked": 0}
    )

    assert "сохранено 2 файла и подтверждено на стороне API" in log
    assert not any("уже было отмечено" in message for message in log)


async def test_a_mixed_confirmation_keeps_both_numbers(redis, sessionmaker) -> None:
    """Here the counters mean something, so they are spelled out."""
    run_id = await make_run(sessionmaker)
    names = [f"{uuid.uuid4()}.txt" for _ in range(2)]

    log = await download_two_names(
        redis, sessionmaker, run_id, names, {"marked_now": 1, "already_marked": 1}
    )

    assert (
        "сохранено 2 файла и подтверждено на стороне API "
        "(новых отметок 1, уже было отмечено 1)" in log
    )


async def test_a_fully_repeated_confirmation_is_reported_and_counts_as_no_progress(
    redis, sessionmaker
) -> None:
    """Everything was already marked: the log says so, and the guard still fires.

    `marked_now = 0` is exactly the shape of the loop the stale-iteration guard
    exists to catch — /names keeps handing out names that never get confirmed.
    """
    run_id = await make_run(sessionmaker)
    names = [f"{uuid.uuid4()}.txt" for _ in range(2)]

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": names})
        mock.post(DOWNLOAD_PATH).mock(
            side_effect=lambda request: httpx.Response(200, content=zip_for(_requested(request)))
        )
        mock.post(DOWNLOADED_PATH).respond(200, json={"marked_now": 0, "already_marked": 2})

        status = await run_loop(run_id, redis, sessionmaker)

    assert status == "failed"
    log = await events(sessionmaker, run_id)
    assert any("уже было отмечено 2" in message for message in log)
    assert any("итерация без прогресса" in message for message in log)

    run = await fetch_run(sessionmaker, run_id)
    assert "без единого подтверждённого" in run.error


@pytest.mark.parametrize(
    ("count", "word"),
    [
        (1, "файл"),
        (2, "файла"),
        (4, "файла"),
        (5, "файлов"),
        (11, "файлов"),
        (14, "файлов"),
        (21, "файл"),
        (22, "файла"),
        (25, "файлов"),
        (111, "файлов"),
    ],
)
def test_the_word_file_is_declined(count: int, word: str) -> None:
    """The log is read by a person: «2 файлов» is a typo, not a message."""
    assert _files_word(count) == word


async def test_the_confirmation_uses_the_declined_word(redis, sessionmaker) -> None:
    """A chunk of one is the case the old wording got wrong most visibly."""
    run_id = await make_run(sessionmaker)
    name = f"{uuid.uuid4()}.txt"

    log = await download_two_names(
        redis, sessionmaker, run_id, [name], {"marked_now": 1, "already_marked": 0}
    )

    assert "сохранено 1 файл и подтверждено на стороне API" in log


async def test_a_lost_lock_stops_the_chunk_immediately(redis, sessionmaker) -> None:
    """Only a per-file failure lets the loop go on; a run-wide one must not.

    The chunk is split name by name, and the lock is stolen while the first of
    them is being handled. The remaining names must not be requested at all.
    """
    run_id = await make_run(sessionmaker)
    names = [f"{index}-{uuid.uuid4()}.txt" for index in range(3)]
    lock = RedisRunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
    await lock.acquire()

    requested_names: list[str] = []

    def download(request: httpx.Request) -> httpx.Response:
        requested = _requested(request)
        requested_names.extend(requested)
        if len(requested) > 1:
            # Reject the whole chunk to force the split.
            return httpx.Response(404, json={"detail": "нет файла"})
        # The slot goes to somebody else while the first single name is in flight.
        asyncio.get_running_loop().create_task(redis.set(LOCK_KEY, token_for(777777)))
        return httpx.Response(200, content=zip_for(requested))

    async with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get(NAMES_PATH).respond(200, json={"file_names": names})
        mock.post(DOWNLOAD_PATH).mock(side_effect=download)
        confirm = mock.post(DOWNLOADED_PATH).respond(
            200, json={"marked_now": 1, "already_marked": 0}
        )

        status = await run_loop(run_id, redis, sessionmaker, lock=lock)

    assert status == "failed"
    assert confirm.call_count == 0
    # The chunk itself plus the first isolated name — and then nothing.
    assert len(requested_names) == len(names) + 1
    run = await fetch_run(sessionmaker, run_id)
    assert "перешёл другому владельцу" in run.error


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
    first = RedisRunLock(redis, run_id=1234, ttl_s=30, heartbeat_s=10)
    assert await first.acquire() is True

    try:
        second = RedisRunLock(redis, run_id=5678, ttl_s=30, heartbeat_s=10)
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
    lock = RedisRunLock(redis, run_id, ttl_s=30, heartbeat_s=0.05)

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
    lock = RedisRunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
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
    lock = RedisRunLock(redis, run_id, ttl_s=30, heartbeat_s=10)
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
