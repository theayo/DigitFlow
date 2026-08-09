"""Run endpoints: starting a download and reporting its state.

Nothing here reaches the external API: the autouse `published_tasks` fixture
swaps the callable `TaskPublisher` dependency for a recording one, so no task
ever reaches the worker container running alongside the suite.
"""

import asyncio
from datetime import timedelta

import pytest
import respx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api import runs as api_runs
from app.config import get_settings
from app.dependencies import get_run_slot, get_task_publisher
from app.main import app
from app.models import DownloadRun, RunEvent
from app.progress import PROGRESS_KEY, Progress, RedisProgressStore
from app.services.runs import SlotUnavailable, claim_run, start_running
from app.time import utc_now
from app.worker import tasks
from app.worker.client import NAMES_PATH
from app.worker.lock import LOCK_KEY, token_for


async def add_run(
    sessionmaker: async_sessionmaker[AsyncSession],
    status: str,
    *,
    age_s: float = 0.0,
    **fields: object,
) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(
            candidate_id="test-api",
            status=status,
            started_at=utc_now() - timedelta(seconds=age_s),
            **fields,
        )
        session.add(run)
        await session.commit()
        return run.id


async def add_working_run(
    sessionmaker: async_sessionmaker[AsyncSession],
    redis,
    status: str = "running",
    **fields: object,
) -> int:
    """A run that is genuinely working: active *and* holding the download lock.

    Both halves are required now that `GET /api/runs/current` reaps orphans: a
    `running` row without the lock is by definition abandoned (§ 8.6), so a test
    that skipped the lock would be describing a dead run.
    """
    run_id = await add_run(sessionmaker, status, **fields)
    await redis.set(LOCK_KEY, token_for(run_id))
    return run_id


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


async def add_events(
    sessionmaker: async_sessionmaker[AsyncSession], run_id: int, messages: list[str]
) -> None:
    async with sessionmaker() as session:
        for message in messages:
            session.add(RunEvent(run_id=run_id, level="info", message=message, ts=utc_now()))
        await session.commit()


# --- POST /api/runs ---------------------------------------------------------


async def test_start_queues_the_task(api, sessionmaker, redis, clean_db, published_tasks) -> None:
    response = await api.post("/api/runs")

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending"
    assert body["active"] is True
    assert body["names_seen"] == 0 and body["files_saved"] == 0
    # The start time is rendered in the display timezone, never in UTC.
    assert body["started_at_nsk"].endswith("+07:00")
    assert published_tasks == [body["run_id"]]

    run = await fetch_run(sessionmaker, body["run_id"])
    assert run.candidate_id == get_settings().candidate_id
    assert [event["message"] for event in body["events"]] == ["задача поставлена в очередь"]


async def test_second_start_is_refused(api, redis, clean_db) -> None:
    first = await api.post("/api/runs")
    second = await api.post("/api/runs")

    assert second.status_code == 409
    assert str(first.json()["run_id"]) in second.json()["detail"]


async def test_running_run_holding_the_lock_blocks_the_start(
    api, sessionmaker, redis, clean_db
) -> None:
    """A live run must not be mistaken for an orphan: it holds the lock."""
    run_id = await add_run(sessionmaker, "running")
    await redis.set(LOCK_KEY, token_for(run_id))

    response = await api.post("/api/runs")

    assert response.status_code == 409
    assert (await fetch_run(sessionmaker, run_id)).status == "running"


async def test_orphan_run_is_reaped_and_the_start_succeeds(
    api, sessionmaker, redis, clean_db
) -> None:
    """Active in the database, no lock in Redis: the worker is gone (§ 8.6)."""
    orphan_id = await add_run(sessionmaker, "running")

    response = await api.post("/api/runs")

    assert response.status_code == 201
    orphan = await fetch_run(sessionmaker, orphan_id)
    assert orphan.status == "failed"
    assert orphan.finished_at is not None
    assert "воркер прекратил работу" in orphan.error


async def test_orphan_holding_a_foreign_lock_is_reaped(api, sessionmaker, redis, clean_db) -> None:
    """The lock belongs to a different run, so this one is not working either."""
    orphan_id = await add_run(sessionmaker, "waiting_retry")
    await redis.set(LOCK_KEY, token_for(orphan_id + 1000))

    response = await api.post("/api/runs")

    assert response.status_code == 201
    assert (await fetch_run(sessionmaker, orphan_id)).status == "failed"


async def test_fresh_pending_run_is_left_alone(api, sessionmaker, redis, clean_db) -> None:
    """A queued run holds no lock yet; killing it on sight would race the worker."""
    run_id = await add_run(sessionmaker, "pending")

    response = await api.post("/api/runs")

    assert response.status_code == 409
    assert (await fetch_run(sessionmaker, run_id)).status == "pending"


async def test_stale_pending_run_is_reaped(api, sessionmaker, redis, clean_db) -> None:
    """Nobody picked the task up within the grace period: nobody ever will."""
    stale_id = await add_run(sessionmaker, "pending", age_s=get_settings().run_pending_grace_s + 60)

    response = await api.post("/api/runs")

    assert response.status_code == 201
    stale = await fetch_run(sessionmaker, stale_id)
    assert stale.status == "failed"
    assert "не была подхвачена воркером" in stale.error


async def test_reaping_writes_a_log_entry(api, sessionmaker, redis, clean_db) -> None:
    orphan_id = await add_run(sessionmaker, "running")

    await api.post("/api/runs")

    async with sessionmaker() as session:
        messages = list(
            (
                await session.execute(
                    select(RunEvent.message).where(
                        RunEvent.run_id == orphan_id, RunEvent.level == "error"
                    )
                )
            ).scalars()
        )
    assert any("осиротевш" in message for message in messages)


async def test_delivery_failure_closes_the_run(api, sessionmaker, redis, clean_db) -> None:
    """A failed publish must not leave a pending run: it may still have been delivered.

    The run is closed as `failed`, which is also a claim on the row — a task that
    turns up later finds a terminal run and does nothing (§ 8.8).
    """

    class BrokenPublisher:
        async def __call__(self, run_id: int) -> None:
            raise RuntimeError("брокер не подтвердил приём")

    # Kept, not discarded: restoring it below brings back the recording publisher
    # rather than the real one, which would try to reach RabbitMQ.
    recording = app.dependency_overrides[get_task_publisher]
    app.dependency_overrides[get_task_publisher] = BrokenPublisher

    response = await api.post("/api/runs")

    assert response.status_code == 503
    async with sessionmaker() as session:
        run = (
            await session.execute(select(DownloadRun).order_by(DownloadRun.id.desc()).limit(1))
        ).scalar_one()
    assert run.status == "failed"
    assert "очередь" in run.error
    assert run.finished_at is not None

    # And the service is usable again straight away: the closed run does not hold
    # the slot.
    app.dependency_overrides[get_task_publisher] = recording
    assert (await api.post("/api/runs")).status_code == 201


class DeliveredThenFailedPublisher:
    """Delivery that got through and then failed to say so.

    The task really does arrive and a worker really does claim the run before the
    exception surfaces — the exact order that makes the failure ambiguous.
    """

    def __init__(self, sessionmaker, *, reach: str) -> None:
        self._sessionmaker = sessionmaker
        self._reach = reach

    async def __call__(self, run_id: int) -> None:
        async with self._sessionmaker() as session:
            assert await claim_run(session, run_id)
            if self._reach == "running":
                assert await start_running(session, run_id)
        raise TimeoutError("подтверждение публикации не получено")


async def assert_ambiguous_delivery_leaves_the_run_alone(
    api, sessionmaker, reach: str, expected_status: str
) -> None:
    recording = app.dependency_overrides[get_task_publisher]
    app.dependency_overrides[get_task_publisher] = lambda: DeliveredThenFailedPublisher(
        sessionmaker, reach=reach
    )
    try:
        response = await api.post("/api/runs")
    finally:
        app.dependency_overrides[get_task_publisher] = recording

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "неоднозначен" in detail
    assert "/api/runs/current" in detail, "клиенту надо сказать, где смотреть фактическое состояние"

    async with sessionmaker() as session:
        run = (
            await session.execute(select(DownloadRun).order_by(DownloadRun.id.desc()).limit(1))
        ).scalar_one()
    # The worker won the row, so the API must not have touched a thing.
    assert run.status == expected_status
    assert run.error is None
    assert run.finished_at is None


async def test_ambiguous_delivery_does_not_close_a_claimed_run(
    api, sessionmaker, redis, clean_db
) -> None:
    """The task arrived and was claimed before the publish call gave up."""
    await assert_ambiguous_delivery_leaves_the_run_alone(
        api, sessionmaker, reach="starting", expected_status="starting"
    )


async def test_ambiguous_delivery_does_not_close_a_working_run(
    api, sessionmaker, redis, clean_db
) -> None:
    """Worse still: by then the run had taken the slot and started working."""
    await assert_ambiguous_delivery_leaves_the_run_alone(
        api, sessionmaker, reach="running", expected_status="running"
    )


# --- concurrent starts ------------------------------------------------------
#
# The 409 above is a fast path, not the guarantee: two requests can both pass it.
# What actually holds the "one run at a time" rule is the partial unique index in
# PostgreSQL (§ 7.1). The Redis lock cannot do it — with `--concurrency 1` the
# worker runs the tasks one after another, so the second run would find the lock
# free by the time it starts and would go straight to the external API.


async def race_two_starts(api, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Fire two starts that both get past the pre-check before either commits."""
    both_checked = asyncio.Barrier(2)
    original = api_runs.find_active_run

    async def checked(session: AsyncSession) -> DownloadRun | None:
        active = await original(session)
        # Neither request may reach its INSERT before the other has finished
        # looking: that is the interleaving the fast path cannot survive.
        await both_checked.wait()
        return active

    monkeypatch.setattr(api_runs, "find_active_run", checked)

    responses = await asyncio.gather(api.post("/api/runs"), api.post("/api/runs"))
    return [response.status_code for response in responses]


async def test_two_concurrent_starts_create_exactly_one_run(
    api, sessionmaker, redis, clean_db, published_tasks, monkeypatch: pytest.MonkeyPatch
) -> None:
    statuses = await race_two_starts(api, monkeypatch)

    assert sorted(statuses) == [201, 409]

    async with sessionmaker() as session:
        runs = list((await session.execute(select(DownloadRun))).scalars())
    assert len(runs) == 1, "второй POST не должен оставлять строку рана"
    assert runs[0].status == "pending"

    # The loser publishes nothing: there is no run of its own to publish.
    assert published_tasks == [runs[0].id]

    # And it leaves no half-created log entry behind either.
    async with sessionmaker() as session:
        events = list((await session.execute(select(RunEvent))).scalars())
    assert [event.run_id for event in events] == [runs[0].id]


async def test_a_losing_start_never_reaches_the_external_api(
    api, sessionmaker, redis, clean_db, published_tasks, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point of the rule: exactly one run talks to the catalog.

    Modelled on the real worker, which runs with `--concurrency 1` and therefore
    processes the queue strictly one task after another — the second task would
    start only once the first had finished and freed the lock.
    """
    assert sorted(await race_two_starts(api, monkeypatch)) == [201, 409]
    assert len(published_tasks) == 1

    async with respx.mock(
        base_url=get_settings().external_api_base_url, assert_all_called=False
    ) as mock:
        names = mock.get(NAMES_PATH).respond(200, json={"file_names": []})

        for run_id in list(published_tasks):
            assert await tasks.execute_run(run_id) == "done"

        assert names.call_count == 1, "второй ран не должен обращаться к каталогу"

    # Nothing was queued behind the finished run.
    assert len(published_tasks) == 1
    async with sessionmaker() as session:
        statuses = list((await session.execute(select(DownloadRun.status))).scalars())
    assert statuses == ["done"]


async def test_an_unrelated_integrity_error_is_not_reported_as_a_conflict(
    api, sessionmaker, redis, clean_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """409 means "a run is already active", not "the database said no".

    Here the status check constraint is the one that fires. Reporting that as a
    conflict would send whoever reads the log looking for a run that never
    existed.
    """

    def run_with_a_bad_status(**fields: object) -> DownloadRun:
        # Short enough for the column, wrong enough for the check constraint.
        return DownloadRun(**{**fields, "status": "нет"})

    monkeypatch.setattr(api_runs, "DownloadRun", run_with_a_bad_status)

    with pytest.raises(IntegrityError) as error:
        await api.post("/api/runs")

    assert "ck_download_run_status" in str(error.value)


# --- GET /api/runs/current --------------------------------------------------


async def test_current_is_null_without_runs(api, redis, clean_db) -> None:
    response = await api.get("/api/runs/current")

    assert response.status_code == 200
    assert response.json() is None


async def test_current_takes_counters_from_live_progress(
    api, sessionmaker, redis, clean_db
) -> None:
    """During a run the counters exist only in Redis: the database gets them at the end."""
    run_id = await add_working_run(sessionmaker, redis)
    await RedisProgressStore(redis).publish(
        Progress(
            run_id=run_id,
            status="running",
            started_at=utc_now(),
            names_seen=9,
            files_saved=6,
        ),
    )

    body = (await api.get("/api/runs/current")).json()

    assert body["run_id"] == run_id
    assert body["names_seen"] == 9
    assert body["files_saved"] == 6
    assert body["active"] is True
    assert body["retry_at"] is None and body["retry_reason"] is None


async def test_current_reports_the_waiting_state(api, sessionmaker, redis, clean_db) -> None:
    """The pause has to be visible, or half an hour of waiting looks like a hang."""
    run_id = await add_working_run(sessionmaker, redis, "waiting_retry")
    retry_at = utc_now() + timedelta(seconds=1800)
    await RedisProgressStore(redis).publish(
        Progress(
            run_id=run_id,
            status="waiting_retry",
            started_at=utc_now(),
            names_seen=9,
            files_saved=3,
            retry_at=retry_at,
            retry_reason="429 от /api/files/names, пауза 1800 с",
        ),
    )

    body = (await api.get("/api/runs/current")).json()

    assert body["status"] == "waiting_retry"
    assert body["retry_reason"].startswith("429")
    # Handed over in UTC: the countdown is computed by the browser.
    assert body["retry_at"].endswith("Z")


async def test_progress_of_another_run_is_ignored(api, sessionmaker, redis, clean_db) -> None:
    """One progress key serves every run, so a stale one must not leak in."""
    run_id = await add_working_run(sessionmaker, redis)
    await RedisProgressStore(redis).publish(
        Progress(
            run_id=run_id + 500,
            status="running",
            started_at=utc_now(),
            names_seen=42,
            files_saved=41,
        ),
    )

    body = (await api.get("/api/runs/current")).json()

    assert body["run_id"] == run_id
    assert body["names_seen"] == 0 and body["files_saved"] == 0


def corrupt_progress(kind: str, run_id: int) -> dict[bytes, bytes]:
    """A progress record for this very run, broken in exactly one way.

    Addressed to the real run on purpose: with a foreign `run_id` the record would
    be discarded by the ownership check and the decoding would never be tested.
    """
    valid = {
        b"run_id": str(run_id).encode(),
        b"status": b"running",
        b"started_at": utc_now().isoformat().encode(),
        b"names_seen": b"9",
        b"files_saved": b"6",
    }
    broken: dict[str, dict[bytes, bytes]] = {
        "нечисловой run_id": {b"run_id": "не число".encode()},
        "наивное время старта": {b"started_at": b"2026-08-09T10:00:00"},
        "битый счётчик": {b"names_seen": "много".encode()},
        "битая отсечка ожидания": {b"retry_at": b"2026-08-09", b"status": b"waiting_retry"},
        "невалидный UTF-8 в значении": {b"status": b"\xff\xfe"},
        "невалидный UTF-8 в ключе": {b"\xff": b"1"},
    }
    return valid | broken[kind]


@pytest.mark.parametrize(
    "kind",
    [
        "нечисловой run_id",
        "наивное время старта",
        "битый счётчик",
        "битая отсечка ожидания",
        "невалидный UTF-8 в значении",
        "невалидный UTF-8 в ключе",
    ],
)
async def test_corrupt_progress_does_not_break_the_page(
    api, sessionmaker, redis, clean_db, kind: str
) -> None:
    """An unreadable record falls back to the database rather than failing the request.

    Nothing from it may leak in either: a broken record must not contribute the
    counters it does carry, nor a zero where PostgreSQL has a real count.
    """
    run_id = await add_working_run(sessionmaker, redis, names_seen=3, files_saved=2)
    await redis.hset(PROGRESS_KEY, mapping=corrupt_progress(kind, run_id))

    response = await api.get("/api/runs/current")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert (body["names_seen"], body["files_saved"]) == (3, 2)
    assert body["retry_at"] is None and body["retry_reason"] is None


async def test_finished_run_reports_stored_counters(api, sessionmaker, redis, clean_db) -> None:
    run_id = await add_run(
        sessionmaker,
        "done",
        names_seen=12,
        files_saved=12,
        finished_at=utc_now(),
    )
    await RedisProgressStore(redis).publish(
        Progress(run_id=run_id, status="done", started_at=utc_now(), names_seen=1, files_saved=1),
    )

    body = (await api.get("/api/runs/current")).json()

    assert body["active"] is False
    assert body["names_seen"] == 12 and body["files_saved"] == 12
    assert body["finished_at_nsk"].endswith("+07:00")


async def test_current_prefers_the_active_run(api, sessionmaker, redis, clean_db) -> None:
    """Two simultaneous starts leave a newer, already closed row behind it."""
    active_id = await add_run(sessionmaker, "running")
    await redis.set(LOCK_KEY, token_for(active_id))
    await add_run(sessionmaker, "failed", finished_at=utc_now())

    body = (await api.get("/api/runs/current")).json()

    assert body["run_id"] == active_id


async def test_current_returns_the_log_tail_oldest_first(
    api, sessionmaker, redis, clean_db
) -> None:
    tail = get_settings().run_events_tail
    run_id = await add_working_run(sessionmaker, redis)
    await add_events(sessionmaker, run_id, [f"событие {index}" for index in range(tail + 5)])

    body = (await api.get("/api/runs/current")).json()

    messages = [event["message"] for event in body["events"]]
    assert len(messages) == tail
    assert messages[0] == "событие 5"
    assert messages[-1] == f"событие {tail + 4}"
    assert body["events"][0]["ts_nsk"].endswith("+07:00")


# --- recovery through the polling endpoint (§ 8.6) --------------------------
#
# Without this the service can deadlock: a dead worker leaves its run active, the
# redelivered task cannot claim it and stands down, and the button that would
# have triggered reaping is disabled precisely because a run is active.


@pytest.mark.parametrize("status", ["running", "waiting_retry"])
async def test_current_reaps_a_run_that_lost_the_lock(
    api, sessionmaker, redis, clean_db, status: str
) -> None:
    run_id = await add_run(sessionmaker, status)

    body = (await api.get("/api/runs/current")).json()

    assert body["run_id"] == run_id
    assert body["status"] == "failed"
    # The whole point: the UI is free to offer a new start again.
    assert body["active"] is False
    assert "воркер прекратил работу" in body["error"]
    assert (await api.post("/api/runs")).status_code == 201


async def test_current_leaves_a_run_holding_its_lock_alone(
    api, sessionmaker, redis, clean_db
) -> None:
    run_id = await add_working_run(sessionmaker, redis)

    body = (await api.get("/api/runs/current")).json()

    assert body["status"] == "running"
    assert body["active"] is True
    assert (await fetch_run(sessionmaker, run_id)).status == "running"


async def test_current_answers_when_the_slot_cannot_be_read(
    api, sessionmaker, redis, clean_db
) -> None:
    """A status page that returns 503 leaves the user with nothing to look at.

    Reaping is skipped — without ownership every active run looks abandoned — and
    the last known state comes from PostgreSQL.
    """

    class UnreachableSlot:
        async def owner(self) -> int | None:
            raise SlotUnavailable("Redis недоступен при чтении lock")

    run_id = await add_run(sessionmaker, "running", names_seen=5, files_saved=4)
    app.dependency_overrides[get_run_slot] = UnreachableSlot
    try:
        response = await api.get("/api/runs/current")
    finally:
        app.dependency_overrides.pop(get_run_slot)

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["status"] == "running"
    assert (body["names_seen"], body["files_saved"]) == (5, 4)
    # And nothing was reaped on a guess.
    assert (await fetch_run(sessionmaker, run_id)).status == "running"


async def test_redelivered_run_recovers_through_polling(api, sessionmaker, redis, clean_db) -> None:
    """End to end: a dead worker's run blocks the UI until polling clears it.

    The redelivered task cannot claim a `running` run, so it stands down without
    touching the external API — and that is exactly the state that used to be a
    dead end.
    """
    run_id = await add_run(sessionmaker, "running")

    async with respx.mock(
        base_url=get_settings().external_api_base_url, assert_all_called=False
    ) as mock:
        names = mock.get(NAMES_PATH)
        assert await tasks.execute_run(run_id) == "ignored"
        assert names.call_count == 0

    # The run is still active, so the download page keeps the button disabled —
    # and the start request is the only *other* place that reaps. Polling is what
    # breaks the circle.
    assert (await fetch_run(sessionmaker, run_id)).status == "running"

    body = (await api.get("/api/runs/current")).json()

    assert body["status"] == "failed" and body["active"] is False
    assert (await api.post("/api/runs")).status_code == 201
