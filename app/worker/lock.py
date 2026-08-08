"""Distributed "one active run" lock.

Kept in Redis rather than checked in the database: a database check is a race
between two simultaneous starts. The token is derived from the run id, so a
Celery retry of the same task recognises its own lock and carries on instead of
deadlocking against itself.

Losing the lock is treated as fatal. If another worker holds the slot it may be
downloading the same names, and a confirmation sent from here could mark a file
that this process never stored.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.worker.errors import LockLost

LOCK_KEY = "dl:lock:run"

# Acquiring must be one round trip. Checking the holder after a failed SET NX and
# then extending it separately leaves a window in which the lock expires and is
# taken over between the two commands.
#   1 - taken fresh, 2 - re-adopted by the same run, 0 - held by someone else
_ACQUIRE_SCRIPT = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return 1
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 2
end
return 0
"""

# Extending and releasing must both verify ownership. A bare PEXPIRE or DEL would
# act on a lock that has already expired and been taken over by another run.
_EXTEND_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def token_for(run_id: int) -> str:
    return f"run:{run_id}"


class RunLock:
    """Ownership of the single download slot."""

    def __init__(self, redis: Redis, run_id: int, ttl_s: float, heartbeat_s: float) -> None:
        self._redis = redis
        self._token = token_for(run_id)
        self._ttl_ms = int(ttl_s * 1000)
        self._heartbeat_s = heartbeat_s
        self._heartbeat: asyncio.Task[None] | None = None
        self._lost = asyncio.Event()
        self._reason: str | None = None

    @property
    def token(self) -> str:
        return self._token

    @property
    def lost(self) -> asyncio.Event:
        """Set once ownership is gone or can no longer be verified."""
        return self._lost

    @property
    def reason(self) -> str | None:
        """Why ownership was lost, for the run diagnostics."""
        return self._reason

    async def acquire(self) -> bool:
        """Take the lock, or re-adopt one this very run already holds.

        Returns True only when the lock is provably ours afterwards: a re-adoption
        counts only if the extension actually landed. Returns False only when the
        slot is genuinely held by another run.

        Raises `LockLost` when Redis cannot answer at all. The two outcomes must
        stay distinguishable: "someone else is downloading" and "we have no idea
        who is downloading" call for different diagnostics.
        """
        try:
            outcome = int(
                await self._redis.eval(_ACQUIRE_SCRIPT, 1, LOCK_KEY, self._token, self._ttl_ms)
            )
        except RedisError as exc:
            self._mark_lost(f"Redis недоступен при захвате lock: {exc}")
            raise LockLost(self._reason or str(exc)) from exc
        return outcome in (1, 2)

    async def holder(self) -> str | None:
        raw = await self._redis.get(LOCK_KEY)
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    async def release(self) -> bool:
        """Release the lock only if we still own it."""
        try:
            removed = await self._redis.eval(_RELEASE_SCRIPT, 1, LOCK_KEY, self._token)
        except RedisError:
            # Nothing to do: the TTL will clear it.
            return False
        return bool(removed)

    async def ensure_owned(self) -> None:
        """Verify ownership against Redis, raising `LockLost` if it is gone.

        Called at every point where losing the slot would cause damage — before
        each external request, before committing, and above all before confirming
        files to the external API.
        """
        if self._lost.is_set():
            raise LockLost(self._reason or "lock рана потерян")

        try:
            holder = await self.holder()
        except RedisError as exc:
            self._mark_lost(f"Redis недоступен при проверке lock: {exc}")
            raise LockLost(self._reason or str(exc)) from exc

        if holder != self._token:
            self._mark_lost(
                f"lock рана перешёл другому владельцу: ожидался {self._token}, сейчас {holder}"
            )
            raise LockLost(self._reason or "lock рана потерян")

    def _mark_lost(self, reason: str) -> None:
        self._reason = reason
        self._lost.set()

    async def _extend(self) -> bool:
        extended = await self._redis.eval(_EXTEND_SCRIPT, 1, LOCK_KEY, self._token, self._ttl_ms)
        return bool(extended)

    async def _heartbeat_loop(self) -> None:
        # The TTL is deliberately shorter than any Retry-After pause, so renewal
        # has to keep running while the run is only waiting.
        while True:
            await asyncio.sleep(self._heartbeat_s)
            try:
                extended = await self._extend()
            except RedisError as exc:
                # Swallowing this inside a background task would leave the run
                # working on a lock it no longer provably holds.
                self._mark_lost(f"Redis недоступен при продлении lock: {exc}")
                return
            except Exception as exc:  # noqa: BLE001 — never lose a heartbeat failure
                self._mark_lost(f"сбой продления lock: {exc!r}")
                return

            if not extended:
                self._mark_lost("lock рана истёк и был перехвачен")
                return

    @asynccontextmanager
    async def hold(self) -> AsyncIterator[bool]:
        """Hold the lock for the duration of the block, releasing it in `finally`."""
        acquired = await self.acquire()
        if not acquired:
            yield False
            return

        self._heartbeat = asyncio.create_task(self._heartbeat_loop())
        try:
            yield True
        finally:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat
            await self.release()
