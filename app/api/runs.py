"""Run endpoints: start a download, report the current one.

The API never talks to the external service, Redis or the broker directly. It
receives those dependencies through FastAPI and coordinates the durable run
with its live progress.
"""

import contextlib
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.dependencies import get_progress_store, get_run_slot, get_task_publisher
from app.models import ACTIVE_RUN_INDEX, DownloadRun, RunEvent
from app.progress import Progress, ProgressStore
from app.schemas import EventOut, RunOut, StartRunRequest
from app.services.runs import (
    RunSlot,
    SlotUnavailable,
    fail_unpublished_run,
    find_active_run,
    is_active,
    latest_run,
    reap_orphan_runs,
)
from app.task_queue import TaskPublisher
from app.time import to_display, utc_now

router = APIRouter(prefix="/api/runs", tags=["runs"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]
PublisherDep = Annotated[TaskPublisher, Depends(get_task_publisher)]
ProgressDep = Annotated[ProgressStore, Depends(get_progress_store)]
SlotDep = Annotated[RunSlot, Depends(get_run_slot)]


@router.post("", response_model=RunOut, status_code=status.HTTP_201_CREATED)
async def start_run(
    session: SessionDep,
    slot: SlotDep,
    publisher: PublisherDep,
    options: StartRunRequest | None = None,
) -> RunOut:
    """Queue a download run, or refuse with 409 while one is already active."""
    settings = get_settings()
    demo = bool(options and options.demo)

    # Checked on the server, not only hidden in the UI: a deployment that did not
    # ask for demo mode must not be talked into the stub catalog by a handmade
    # request.
    if demo and not settings.demo_mode:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="демонстрационный режим выключен: включите DEMO_MODE в окружении",
        )

    try:
        # A worker that died left its run marked active, and that row would block
        # every future start. Cleared here, before the slot is considered taken.
        await _reap(session, slot)
    except SlotUnavailable as exc:
        # Starting a run without knowing who owns the slot would mean guessing,
        # and the guess costs either a duplicated download or a run killed while
        # it works. Refusing is the only honest answer.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"владельца слота не установить, запустить выкачку нельзя: {exc}",
        ) from exc

    # Fast path only: this answers the ordinary "a run is already going" case with
    # a helpful message. It is not what makes the rule hold — two requests can
    # both get past it — and the insert below is.
    active = await find_active_run(session)
    if active is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"ран {active.id} уже выполняется (статус {active.status})",
        )

    run = DownloadRun(
        candidate_id=settings.candidate_id,
        status="pending",
        started_at=utc_now(),
        demo=demo,
    )
    try:
        session.add(run)
        # Flushed rather than committed: the log entry needs the id, and the run
        # and its first event belong in one transaction. The flush is also where
        # the partial unique index rejects a second active run, so the rollback
        # below takes the event with it and nothing half-created is left behind.
        await session.flush()
        session.add(
            RunEvent(
                run_id=run.id, level="info", message="задача поставлена в очередь", ts=utc_now()
            )
        )
        if demo:
            # Said out loud in the run's own log, so a stub run cannot later be
            # read — or screenshotted — as a real download.
            session.add(
                RunEvent(
                    run_id=run.id,
                    level="warning",
                    message="демонстрационный режим: ран пойдёт в заглушку, а не в боевое API",
                    ts=utc_now(),
                )
            )
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        if not _is_active_run_conflict(exc):
            # Some other constraint failed. Reporting that as "a run is already
            # active" would send whoever reads it looking for a run that is not
            # there, so it stays a 500 with the real error.
            raise
        # Another request won the race. Nothing is published, because nothing was
        # created — the loser must not queue a task for a run that does not exist.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="ран уже запущен параллельным запросом",
        ) from exc

    try:
        await publisher(run.id)
    except Exception as exc:  # noqa: BLE001 — any delivery failure has to be resolved
        # A failed publish is not proof of a failed delivery: the broker may have
        # accepted the task and only failed to confirm it, and by now a worker can
        # already have claimed the run. So this is a race, not bookkeeping, and it
        # is resolved by a transition out of `pending` only — never by closing
        # "whatever active run is there" (§ 8.8).
        closed = await fail_unpublished_run(
            session, run.id, f"не удалось поставить задачу в очередь: {exc}"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"очередь задач недоступна: {exc}. Ран закрыт; если задача всё же дошла, "
                "она будет проигнорирована"
            )
            if closed
            # We lost the race, which means the task did arrive and the run is
            # somebody's work now. It keeps running, and the client is told where
            # to look instead of being told it failed.
            else (
                f"результат публикации неоднозначен: {exc}. Задача, судя по всему, дошла — "
                "ран уже подхвачен воркером и продолжает работу. "
                "Фактическое состояние — GET /api/runs/current"
            ),
        ) from exc

    return _serialize(run, progress=None, events=await _tail(session, run.id))


@router.get("/current", response_model=RunOut | None)
async def current_run(
    session: SessionDep, slot: SlotDep, progress_store: ProgressDep
) -> RunOut | None:
    """Report the running run, or the last finished one, or null if there is none.

    Also the service's way out of a dead end. A worker that dies leaves its run
    active; the redelivered task cannot claim it and stands down; the button stays
    disabled because a run is active — and the only other place that reaps orphans
    is the very request the button would have sent (§ 8.6). Since the download
    page polls this endpoint, reaping here closes that loop on its own.

    An active run outranks a newer terminal row on purpose. Such a row may be
    created by an unrelated later attempt or administrative recovery; reporting
    it would hide the download that is actually in progress. Concurrent starts
    cannot create that situation because the database rejects the losing insert.
    """
    # Unlike the start endpoint, this one must answer no matter what: a status
    # page that returns 503 leaves the user with no way to see what is going on.
    # Without ownership there is simply nothing to reap, and PostgreSQL still
    # holds the last known state.
    with contextlib.suppress(SlotUnavailable):
        await _reap(session, slot)

    run = await find_active_run(session) or await latest_run(session)
    if run is None:
        return None

    # Never raises by contract: an unreachable or unreadable store yields None,
    # and the page still gets the status and the log from PostgreSQL.
    progress = await progress_store.read()

    return _serialize(run, progress, await _tail(session, run.id))


def _is_active_run_conflict(error: IntegrityError) -> bool:
    """Tell the "one active run" index apart from any other integrity failure.

    Matched by constraint name rather than by exception type: `IntegrityError`
    also covers the status check constraint and the foreign key, and turning
    those into a 409 would be a lie.
    """
    cause = error.orig
    name = getattr(getattr(cause, "__cause__", None), "constraint_name", None)
    return name == ACTIVE_RUN_INDEX or ACTIVE_RUN_INDEX in str(cause)


async def _reap(session: AsyncSession, slot: RunSlot) -> list[int]:
    """Close abandoned runs, with the grace periods from the configuration."""
    settings = get_settings()
    return await reap_orphan_runs(
        session,
        slot,
        pending_grace_s=settings.run_pending_grace_s,
        starting_grace_s=settings.run_starting_grace_s,
    )


async def _tail(session: AsyncSession, run_id: int) -> list[RunEvent]:
    """Return the last log entries of a run, oldest first."""
    rows = (
        (
            await session.execute(
                select(RunEvent)
                .where(RunEvent.run_id == run_id)
                .order_by(RunEvent.id.desc())
                .limit(get_settings().run_events_tail)
            )
        )
        .scalars()
        .all()
    )
    return list(reversed(rows))


def _serialize(
    run: DownloadRun,
    progress: Progress | None,
    events: list[RunEvent],
) -> RunOut:
    """Merge the stored run with the live progress published by the worker."""
    # Progress is a single record for the whole service, so it may well describe
    # an older run. Only the matching one may contribute anything.
    live = progress if progress is not None and progress.run_id == run.id else None

    names_seen, files_saved = run.names_seen, run.files_saved
    if live is not None and is_active(run.status):
        # The counters reach the database only when the run ends; until then the
        # progress store is the only place they exist.
        names_seen = live.names_seen
        files_saved = live.files_saved

    waiting = live is not None and run.status == "waiting_retry"

    return RunOut(
        run_id=run.id,
        status=run.status,
        active=is_active(run.status),
        demo=run.demo,
        started_at_nsk=to_display(run.started_at),
        finished_at_nsk=to_display(run.finished_at) if run.finished_at else None,
        names_seen=names_seen,
        files_saved=files_saved,
        # Left in UTC on purpose: the countdown is the browser's job.
        retry_at=live.retry_at if waiting else None,
        retry_reason=live.retry_reason if waiting else None,
        error=run.error,
        events=[
            EventOut(
                id=event.id,
                ts_nsk=to_display(event.ts),
                level=event.level,
                message=event.message,
            )
            for event in events
        ],
    )
