"""Live progress of a download run and its Redis storage.

The worker publishes rapidly changing counters here while PostgreSQL keeps the
durable run and event log.  The small protocol stays beside the value it stores,
so callers can use a test double without a separate ports/adapters hierarchy.
"""

import contextlib
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.time import ensure_aware

PROGRESS_KEY = "dl:progress"

# Long enough to survive a full 30-minute ban plus the pause that follows it.
PROGRESS_TTL_S = 4 * 60 * 60

_REQUIRED = ("run_id", "status", "started_at")


@dataclass(frozen=True)
class Progress:
    """Everything the download page needs besides the event log."""

    run_id: int
    status: str
    started_at: datetime
    names_seen: int = 0
    files_saved: int = 0
    retry_at: datetime | None = None
    retry_reason: str | None = None


class ProgressStore(Protocol):
    """Storage contract used by the API and the download runner."""

    async def publish(self, progress: Progress) -> None:
        """Store progress on a best-effort basis."""

    async def read(self) -> Progress | None:
        """Return current progress, or None when it is absent or unreadable."""


class RedisProgressStore:
    """Progress of the current run, stored as a single Redis hash."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, progress: Progress) -> None:
        """Replace the stored progress without making display failure fatal."""
        with contextlib.suppress(RedisError):
            async with self._redis.pipeline(transaction=True) as pipe:
                # Delete first so retry fields disappear when the run resumes.
                pipe.delete(PROGRESS_KEY)
                pipe.hset(PROGRESS_KEY, mapping=_encode(progress))
                pipe.expire(PROGRESS_KEY, PROGRESS_TTL_S)
                await pipe.execute()

    async def read(self) -> Progress | None:
        """Return progress, swallowing Redis and decoding failures."""
        try:
            raw = await self._redis.hgetall(PROGRESS_KEY)
            if not raw:
                return None
            mapping = {_text(key): _text(value) for key, value in raw.items()}
        except (RedisError, UnicodeDecodeError):
            return None
        return _decode(mapping)


def _encode(progress: Progress) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for key, value in vars(progress).items():
        if value is None:
            continue
        mapping[key] = value.isoformat() if isinstance(value, datetime) else str(value)
    return mapping


def _decode(mapping: dict[str, str]) -> Progress | None:
    """Rebuild a complete trustworthy progress record, or reject it."""
    if any(field not in mapping for field in _REQUIRED):
        return None

    run_id = _int(mapping["run_id"])
    started_at = _datetime(mapping["started_at"])
    if run_id is None or started_at is None:
        return None

    names_seen = _counter(mapping, "names_seen")
    files_saved = _counter(mapping, "files_saved")
    if names_seen is None or files_saved is None:
        return None

    retry_at = _datetime(mapping.get("retry_at"))
    if "retry_at" in mapping and retry_at is None:
        return None

    return Progress(
        run_id=run_id,
        status=mapping["status"],
        started_at=started_at,
        names_seen=names_seen,
        files_saved=files_saved,
        retry_at=retry_at,
        retry_reason=mapping.get("retry_reason"),
    )


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _counter(mapping: dict[str, str], field: str) -> int | None:
    if field not in mapping:
        return 0
    return _int(mapping[field])


def _int(raw: str | None) -> int | None:
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


def _datetime(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    try:
        return ensure_aware(datetime.fromisoformat(raw))
    except ValueError:
        return None
