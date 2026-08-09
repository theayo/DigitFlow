"""Pacing of outgoing requests to the external API: the Redis side of `RateLimiter`.

The service publishes neither its rate limit nor its window, so the pace is ours
to choose: at least `external_min_interval_ms` between any two consecutive
requests. The state lives in Redis rather than in the process so that the limit
holds even when more than one worker is running.

After every 429 the interval doubles up to a ceiling and is not lowered again
until the run ends: when the real limit is unknown, approaching it from below is
much cheaper than collecting a 30-minute ban.
"""

from redis.asyncio import Redis

NEXT_ALLOWED_KEY = "dl:ratelimit:next_allowed_ms"
INTERVAL_KEY = "dl:ratelimit:interval_ms"

# Reserving a slot must be atomic: two workers reading the same "next allowed"
# moment would both decide they may go now.
_RESERVE_SCRIPT = """
local now = tonumber(ARGV[1])
local default_interval = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local interval = tonumber(redis.call('GET', KEYS[2])) or default_interval
local next_allowed = tonumber(redis.call('GET', KEYS[1])) or 0
local start = now
if next_allowed > start then
    start = next_allowed
end

redis.call('SET', KEYS[1], start + interval, 'PX', ttl)
return start - now
"""


class RedisRateLimiter:
    """Reserves send slots so that requests never come closer than the interval."""

    def __init__(
        self,
        redis: Redis,
        min_interval_ms: int,
        max_interval_ms: int,
        max_retry_wait_s: int,
    ) -> None:
        self._redis = redis
        self._min_interval_ms = min_interval_ms
        self._max_interval_ms = max_interval_ms
        # The penalty has to survive the longest pause the run may sit through:
        # a 30-minute ban must not silently reset the pace we learned from a 429.
        # Cleared explicitly when a run ends; the TTL is only the crash safety net.
        self._ttl_ms = max(max_retry_wait_s * 2, 3600) * 1000

    async def reset(self) -> None:
        """Drop the accumulated penalty. Called when a run starts and when it ends."""
        await self._redis.delete(NEXT_ALLOWED_KEY, INTERVAL_KEY)

    async def penalty_ttl_ms(self) -> int:
        """Remaining lifetime of the penalty, in milliseconds. -2 when absent."""
        return int(await self._redis.pttl(INTERVAL_KEY))

    async def current_interval_ms(self) -> int:
        raw = await self._redis.get(INTERVAL_KEY)
        return int(raw) if raw is not None else self._min_interval_ms

    async def reserve(self) -> float:
        """Reserve the next slot and return how long to wait, in seconds."""
        now_ms = await self._now_ms()
        wait_ms = await self._redis.eval(
            _RESERVE_SCRIPT,
            2,
            NEXT_ALLOWED_KEY,
            INTERVAL_KEY,
            now_ms,
            self._min_interval_ms,
            self._ttl_ms,
        )
        return max(0.0, float(wait_ms) / 1000.0)

    async def penalize(self) -> int:
        """Double the interval after a 429. Returns the new interval."""
        current = await self.current_interval_ms()
        updated = min(current * 2, self._max_interval_ms)
        await self._redis.set(INTERVAL_KEY, updated, px=self._ttl_ms)
        return updated

    async def _now_ms(self) -> int:
        """Take the clock from Redis so that several workers share one timeline."""
        seconds, microseconds = await self._redis.time()
        return int(seconds) * 1000 + int(microseconds) // 1000
