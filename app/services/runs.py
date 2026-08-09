"""The run state machine (§ 8.8), shared by the API and the Celery task.

Every transition lives here, and every one of them is a conditional UPDATE
rather than a read followed by an unconditional write. Two writers can reach the
same row — a redelivered task, the orphan reaper, the API closing a run it could
not publish — and the losing writer must miss rather than overwrite.

Three rules carry the weight:

- `done` and `failed` are terminal. Nothing resurrects them, so a task delivered
  after its run was already closed does nothing at all;
- a run is claimed for execution exactly once, by moving it out of `pending`.
  The lock cannot arbitrate that, because two deliveries of the same run compute
  the same token and would both be let through;
- `starting` exists so that "claimed" and "provably holding the slot" are
  different states. `running` is entered only after ownership of the lock has
  been verified, which is what lets the reaper treat a `running` run without the
  lock as dead without ever hitting a run that is merely mid-startup.

Reaping orphans (§ 8.6) is the other half: a worker that dies leaves a row
claiming to be active forever, and since only one run may be active at a time,
that row blocks every future start. What tells an orphan from a live run depends
on the state — ownership for `running` and `waiting_retry`, which are supposed to
hold the lock, and time alone for `pending` and `starting`, which are not.
"""

from datetime import datetime
from typing import Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DownloadRun, RunEvent
from app.time import ensure_aware, utc_now


class SlotUnavailable(RuntimeError):
    """The owner of the download slot cannot be determined safely."""


class RunSlot(Protocol):
    """Read-only view of the single download slot used by orphan reaping."""

    async def owner(self) -> int | None:
        """Return the owning run id, None when free, or raise SlotUnavailable."""


# A run in any of these states is "active": it occupies the slot and blocks a new
# start.
ACTIVE_STATUSES: tuple[str, ...] = ("pending", "starting", "running", "waiting_retry")

# Active states that are expected to hold the download lock. The other two are
# on their way to it and are protected by a grace period instead.
LOCK_HOLDING_STATUSES: tuple[str, ...] = ("running", "waiting_retry")

# Terminal states. A run that reached one keeps its outcome forever.
TERMINAL_STATUSES: tuple[str, ...] = ("done", "failed")


def is_active(status: str) -> bool:
    return status in ACTIVE_STATUSES


async def latest_run(session: AsyncSession) -> DownloadRun | None:
    """Return the most recently created run, or None if there has never been one."""
    return (
        await session.execute(select(DownloadRun).order_by(DownloadRun.id.desc()).limit(1))
    ).scalar_one_or_none()


async def find_active_run(session: AsyncSession) -> DownloadRun | None:
    """Return the run currently occupying the slot, if any."""
    return (
        await session.execute(
            select(DownloadRun)
            .where(DownloadRun.status.in_(ACTIVE_STATUSES))
            .order_by(DownloadRun.id)
            .limit(1)
        )
    ).scalar_one_or_none()


async def claim_run(session: AsyncSession, run_id: int) -> bool:
    """Take a pending run for this delivery: `pending` -> `starting`.

    True only for the caller that won it. Atomic on purpose: delivery is
    at-least-once and can be ambiguous, so two executors may well arrive with the
    same `run_id`. The loser gets False and must return without touching the
    external API — the winner is already working on it.

    Winning the claim is not permission to download. It only reserves the run;
    the slot still has to be taken and verified, and `start_running()` is what
    turns that into a working run.

    A run that is no longer pending is never claimed: anything active means
    someone else took it, and `done` or `failed` mean the outcome is settled and
    reviving it would overwrite a recorded result.
    """
    result = await session.execute(
        update(DownloadRun)
        .where(DownloadRun.id == run_id, DownloadRun.status == "pending")
        .values(status="starting", started_at=utc_now())
    )
    await session.commit()
    return result.rowcount == 1


async def start_running(session: AsyncSession, run_id: int) -> bool:
    """Begin work on a claimed run: `starting` -> `running`.

    Called only once the lock is held *and* ownership has been verified, so that
    `running` in the database means "this run provably owns the slot". The reaper
    depends on exactly that: it closes a lock-less `running` run without waiting.

    False means the run was closed while it was starting — by the reaper on an
    expired startup grace, or by anything else. The caller must then release the
    lock and stop, without a single external request.
    """
    result = await session.execute(
        update(DownloadRun)
        .where(DownloadRun.id == run_id, DownloadRun.status == "starting")
        .values(status="running")
    )
    await session.commit()
    return result.rowcount == 1


async def set_run_status(session: AsyncSession, run_id: int, status: str) -> bool:
    """Move a working run between `running` and `waiting_retry`.

    Guards the pause from both sides: a run closed by the reaper while it was
    waiting must not be dragged back into `waiting_retry` or `running`, and a run
    that never reached `running` must not skip into either.
    """
    result = await session.execute(
        update(DownloadRun)
        .where(DownloadRun.id == run_id, DownloadRun.status.in_(LOCK_HOLDING_STATUSES))
        .values(status=status)
    )
    await session.commit()
    return result.rowcount == 1


async def finish_run(
    session: AsyncSession,
    run_id: int,
    *,
    status: str,
    error: str | None,
    expected: tuple[str, ...] = ACTIVE_STATUSES,
    names_seen: int | None = None,
    files_saved: int | None = None,
    event: str | None = None,
    event_level: str = "error",
) -> bool:
    """Close a run, optionally logging why. False if it was not in `expected`.

    Counters are written in the same conditional update as the status, so a run
    that is already terminal keeps both its outcome and the numbers that belong
    to it.

    `expected` narrows the source states. Widening it to every active state is
    right only for a caller that owns the run — the runner closing its own work,
    or the task adapter closing a run it has just claimed. A caller racing an
    owner must name the exact state it believes the run to be in, or it will
    happily close a run somebody else is working on.
    """
    values: dict[str, object] = {"status": status, "finished_at": utc_now(), "error": error}
    if names_seen is not None:
        values["names_seen"] = names_seen
    if files_saved is not None:
        values["files_saved"] = files_saved

    result = await session.execute(
        update(DownloadRun)
        .where(DownloadRun.id == run_id, DownloadRun.status.in_(expected))
        .values(**values)
    )
    if result.rowcount != 1:
        await session.rollback()
        return False

    if event is not None:
        session.add(RunEvent(run_id=run_id, level=event_level, message=event, ts=utc_now()))
    await session.commit()
    return True


async def fail_unpublished_run(session: AsyncSession, run_id: int, reason: str) -> bool:
    """Close a run whose task could not be published: `pending` -> `failed` only.

    A failed publish does not mean the task was not delivered — the broker may
    have accepted it and only failed to confirm. By the time the exception
    surfaces, a worker can already have claimed the run and be working on it.

    Hence the exact source state. False means precisely that: the delivery got
    through after all, the run is somebody's work now, and its status, error,
    counters and finish time must be left exactly as they are.
    """
    return await finish_run(
        session,
        run_id,
        status="failed",
        error=reason,
        expected=("pending",),
        event=reason,
    )


async def reap_orphan_runs(
    session: AsyncSession,
    slot: RunSlot,
    *,
    pending_grace_s: float,
    starting_grace_s: float,
) -> list[int]:
    """Close runs whose worker stopped working. Returns the ids that were closed.

    Raises `SlotUnavailable` when ownership cannot be read: without knowing who
    owns the slot every active run looks like an orphan, and reaping a live one
    would declare a working download dead.
    """
    candidates = (
        await session.execute(
            select(DownloadRun.id, DownloadRun.status, DownloadRun.started_at)
            .where(DownloadRun.status.in_(ACTIVE_STATUSES))
            .order_by(DownloadRun.id)
        )
    ).all()
    if not candidates:
        return []

    # Read after the database, not before. A run is only moved to `running` after
    # its worker has verified that it owns the lock, so in this order a run that
    # is still taking the slot is seen as `pending` or `starting` — both of which
    # are judged by time, not by ownership. The opposite order could see it as
    # `running` against a lock read from before it was taken, and kill it.
    owner = await slot.owner()

    now = utc_now()
    reaped: list[int] = []

    for run_id, status, started_at in candidates:
        if run_id == owner:
            continue

        reason = _orphan_reason(status, started_at, now, pending_grace_s, starting_grace_s)
        if reason is None:
            continue

        # Matched on the exact status observed above: a run that changed state in
        # the meantime is alive after all, and this update has to miss it. That
        # also covers a run which finished normally between the two reads.
        result = await session.execute(
            update(DownloadRun)
            .where(DownloadRun.id == run_id, DownloadRun.status == status)
            .values(status="failed", finished_at=now, error=reason)
        )
        if result.rowcount != 1:
            continue

        session.add(
            RunEvent(
                run_id=run_id,
                level="error",
                message=f"ран закрыт как осиротевший: {reason}",
                ts=now,
            )
        )
        reaped.append(run_id)

    if reaped:
        await session.commit()
    return reaped


def _orphan_reason(
    status: str,
    started_at: datetime,
    now: datetime,
    pending_grace_s: float,
    starting_grace_s: float,
) -> str | None:
    """Explain why this run is an orphan, or None if it must be left alone.

    Reached only for runs that do not own the lock, so each state is judged by
    whether it was supposed to own it in the first place.
    """
    if status in LOCK_HOLDING_STATUSES:
        # These states are entered only after ownership was verified, so a run
        # sitting in one without the lock has lost it: the worker is gone, or its
        # lock expired without a heartbeat.
        return (
            "воркер прекратил работу: ран числится активным, но lock скачивания ему не принадлежит"
        )

    age_s = (now - ensure_aware(started_at)).total_seconds()

    if status == "starting":
        # Claimed, but the slot is not taken yet. Absence of the lock proves
        # nothing here — the worker may be a few milliseconds away from taking
        # it — so only a stalled startup counts.
        if age_s < starting_grace_s:
            return None
        return (
            f"ран не смог начать работу за {age_s:.0f} с (предел — {starting_grace_s:.0f} с): "
            "воркер не захватил lock скачивания"
        )

    # A pending run has not even been picked up. Only time says anything: the
    # task is either still queued or was never delivered.
    if age_s < pending_grace_s:
        return None
    return (
        f"задача не была подхвачена воркером за {age_s:.0f} с "
        f"(предел — {pending_grace_s:.0f} с): очередь недоступна или воркер не запущен"
    )
