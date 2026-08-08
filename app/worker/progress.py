"""Live run progress, published to Redis.

Counters change on every step of the loop, so they do not belong in Postgres.
The database keeps the run itself and its event log; this hash is what the
download page polls.
"""

from dataclasses import asdict, dataclass
from datetime import datetime

from redis.asyncio import Redis

PROGRESS_KEY = "dl:progress"

# Long enough to survive a full 30-minute ban plus the pause that follows it.
PROGRESS_TTL_S = 4 * 60 * 60


@dataclass
class Progress:
    """Everything the download page needs besides the event log."""

    run_id: int
    status: str
    started_at: datetime
    names_seen: int = 0
    files_saved: int = 0
    retry_at: datetime | None = None
    retry_reason: str | None = None

    def as_mapping(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for key, value in asdict(self).items():
            if value is None:
                continue
            mapping[key] = value.isoformat() if isinstance(value, datetime) else str(value)
        return mapping


async def publish(redis: Redis, progress: Progress) -> None:
    """Replace the stored progress with the current one."""
    mapping = progress.as_mapping()
    async with redis.pipeline(transaction=True) as pipe:
        # Deleted first: retry_at and retry_reason must disappear once the run
        # stops waiting, and HSET alone would leave the stale values behind.
        pipe.delete(PROGRESS_KEY)
        pipe.hset(PROGRESS_KEY, mapping=mapping)
        pipe.expire(PROGRESS_KEY, PROGRESS_TTL_S)
        await pipe.execute()


async def read(redis: Redis) -> dict[str, str] | None:
    """Return the stored progress, or None if no run has published any."""
    raw = await redis.hgetall(PROGRESS_KEY)
    if not raw:
        return None
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in raw.items()
    }
