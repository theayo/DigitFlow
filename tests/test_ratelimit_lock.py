"""Integration tests for the Redis-backed pacing and run lock."""

import asyncio

import pytest
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.worker.errors import LockLost
from app.worker.lock import LOCK_KEY, RedisRunLock, token_for
from app.worker.ratelimit import INTERVAL_KEY, RedisRateLimiter

MAX_RETRY_WAIT_S = 1900


def build_limiter(
    redis: aioredis.Redis,
    minimum: int = 200,
    maximum: int = 800,
    max_retry_wait_s: int = MAX_RETRY_WAIT_S,
) -> RedisRateLimiter:
    return RedisRateLimiter(
        redis,
        min_interval_ms=minimum,
        max_interval_ms=maximum,
        max_retry_wait_s=max_retry_wait_s,
    )


# --- pacing -----------------------------------------------------------------


async def test_first_slot_is_immediate(redis: aioredis.Redis) -> None:
    limiter = build_limiter(redis)

    assert await limiter.reserve() == 0.0


async def test_second_slot_waits_the_interval(redis: aioredis.Redis) -> None:
    limiter = build_limiter(redis, minimum=500)

    await limiter.reserve()
    wait = await limiter.reserve()

    assert 0.3 <= wait <= 0.5


async def test_penalty_doubles_up_to_the_ceiling(redis: aioredis.Redis) -> None:
    limiter = build_limiter(redis, minimum=200, maximum=800)

    assert await limiter.penalize() == 400
    assert await limiter.penalize() == 800
    assert await limiter.penalize() == 800


async def test_reset_clears_the_penalty(redis: aioredis.Redis) -> None:
    limiter = build_limiter(redis, minimum=200, maximum=800)
    await limiter.penalize()

    await limiter.reset()

    assert await limiter.current_interval_ms() == 200


async def test_penalty_outlives_the_longest_possible_pause(redis: aioredis.Redis) -> None:
    """A 30-minute ban must not quietly reset the pace learned from a 429."""
    limiter = build_limiter(redis, max_retry_wait_s=MAX_RETRY_WAIT_S)
    await limiter.penalize()

    ttl_ms = await limiter.penalty_ttl_ms()

    assert ttl_ms > MAX_RETRY_WAIT_S * 1000


async def test_penalty_survives_a_pause_longer_than_the_old_ttl(
    redis: aioredis.Redis,
) -> None:
    """Simulates the pause by ageing the key instead of waiting it out.

    The previous TTL was ~150 s; the key is aged past that and the penalty must
    still be in force.
    """
    limiter = build_limiter(redis, minimum=200, maximum=6400)
    await limiter.penalize()

    aged_out_previous_ttl = MAX_RETRY_WAIT_S * 1000 - 150_000
    remaining = await limiter.penalty_ttl_ms() - aged_out_previous_ttl
    assert remaining > 0
    await redis.pexpire(INTERVAL_KEY, remaining)

    assert await limiter.current_interval_ms() == 400


# --- lock -------------------------------------------------------------------


async def test_lock_is_exclusive_between_runs(redis: aioredis.Redis) -> None:
    first = RedisRunLock(redis, run_id=1, ttl_s=30, heartbeat_s=10)
    second = RedisRunLock(redis, run_id=2, ttl_s=30, heartbeat_s=10)

    assert await first.acquire() is True
    assert await second.acquire() is False

    await first.release()
    assert await second.acquire() is True
    await second.release()


async def test_same_run_readopts_its_own_lock(redis: aioredis.Redis) -> None:
    """A Celery retry of the same run must continue, not deadlock against itself."""
    lock = RedisRunLock(redis, run_id=7, ttl_s=30, heartbeat_s=10)
    await lock.acquire()

    retry = RedisRunLock(redis, run_id=7, ttl_s=30, heartbeat_s=10)

    assert await retry.acquire() is True
    await retry.release()


async def test_owner_change_between_attempts_blocks_acquire(redis: aioredis.Redis) -> None:
    """The lock expired and someone else took it: re-adoption must not succeed."""
    lock = RedisRunLock(redis, run_id=7, ttl_s=30, heartbeat_s=10)
    assert await lock.acquire() is True

    # Exactly what an expiry followed by a takeover looks like from Redis.
    await redis.set(LOCK_KEY, token_for(8))

    assert await lock.acquire() is False
    assert await lock.holder() == token_for(8)


async def test_release_does_not_touch_someone_elses_lock(redis: aioredis.Redis) -> None:
    await redis.set(LOCK_KEY, token_for(99))
    stranger = RedisRunLock(redis, run_id=100, ttl_s=30, heartbeat_s=10)

    assert await stranger.release() is False
    assert await stranger.holder() == token_for(99)


async def test_release_after_takeover_keeps_the_new_owner(redis: aioredis.Redis) -> None:
    """Our own release must not evict the run that legitimately took over."""
    lock = RedisRunLock(redis, run_id=42, ttl_s=30, heartbeat_s=10)
    await lock.acquire()

    await redis.set(LOCK_KEY, token_for(43))

    assert await lock.release() is False
    assert await lock.holder() == token_for(43)


async def test_lock_is_released_when_the_body_raises(redis: aioredis.Redis) -> None:
    lock = RedisRunLock(redis, run_id=5, ttl_s=30, heartbeat_s=10)

    try:
        async with lock.hold() as acquired:
            assert acquired
            raise RuntimeError("сбой посреди рана")
    except RuntimeError:
        pass

    assert await lock.holder() is None


async def test_heartbeat_extends_the_ttl(redis: aioredis.Redis) -> None:
    """The TTL is shorter than a Retry-After pause, so renewal must keep working."""
    lock = RedisRunLock(redis, run_id=11, ttl_s=1, heartbeat_s=0.3)

    async with lock.hold() as acquired:
        assert acquired
        await asyncio.sleep(1.2)
        assert await lock.holder() == token_for(11)

    assert await lock.holder() is None


async def test_ownership_check_detects_a_stolen_lock(redis: aioredis.Redis) -> None:
    lock = RedisRunLock(redis, run_id=21, ttl_s=30, heartbeat_s=10)
    await lock.acquire()
    await lock.ensure_owned()

    await redis.set(LOCK_KEY, token_for(22))

    with pytest.raises(LockLost, match="перешёл другому владельцу"):
        await lock.ensure_owned()


async def test_heartbeat_redis_failure_marks_the_lock_lost(
    redis: aioredis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Redis error inside the background task must not vanish silently."""
    lock = RedisRunLock(redis, run_id=31, ttl_s=30, heartbeat_s=0.05)

    async def broken_extend() -> bool:
        raise RedisError("соединение потеряно")

    monkeypatch.setattr(lock, "_extend", broken_extend)

    async with lock.hold() as acquired:
        assert acquired
        await asyncio.sleep(0.2)

        assert lock.lost.is_set()
        assert lock.reason is not None
        assert "Redis недоступен" in lock.reason
        with pytest.raises(LockLost):
            await lock.ensure_owned()
