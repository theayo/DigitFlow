"""The Redis hash behind `ProgressStore`.

Runs against the test Redis: what is worth checking here is the encoding of a
real record and the refusal to trust a damaged one, and both only mean something
against a real server.
"""

from datetime import timedelta

import pytest
from redis.exceptions import RedisError

from app.progress import PROGRESS_KEY, Progress, RedisProgressStore
from app.time import utc_now

# --- progress store ---------------------------------------------------------


def valid_hash() -> dict[str, bytes]:
    """A record that reads back fine, so a test can corrupt exactly one field."""
    return {
        "run_id": b"42",
        "status": b"running",
        "started_at": utc_now().isoformat().encode(),
        "names_seen": b"9",
        "files_saved": b"6",
    }


def sample(**overrides: object) -> Progress:
    values: dict = {
        "run_id": 42,
        "status": "running",
        "started_at": utc_now(),
        "names_seen": 9,
        "files_saved": 6,
    }
    values.update(overrides)
    return Progress(**values)


async def test_progress_survives_a_round_trip(redis) -> None:
    progress = sample(
        status="waiting_retry",
        retry_at=utc_now() + timedelta(seconds=30),
        retry_reason="429 от /api/files/names",
    )

    await RedisProgressStore(redis).publish(progress)

    assert await RedisProgressStore(redis).read() == progress


async def test_waiting_fields_disappear_when_the_run_resumes(redis) -> None:
    """HSET alone would leave the old pause behind and the page would keep counting down."""
    store = RedisProgressStore(redis)
    await store.publish(sample(status="waiting_retry", retry_at=utc_now(), retry_reason="429"))

    await store.publish(sample(status="running"))

    stored = await store.read()
    assert stored is not None
    assert stored.retry_at is None and stored.retry_reason is None


async def test_no_progress_reads_as_none(redis) -> None:
    assert await RedisProgressStore(redis).read() is None


async def test_corrupt_record_reads_as_none(redis) -> None:
    """A half-written or outdated record must not take the download page down."""
    await redis.hset(PROGRESS_KEY, mapping={"run_id": "не число", "status": "running"})

    assert await RedisProgressStore(redis).read() is None


async def test_naive_start_time_reads_as_none(redis) -> None:
    """A timestamp without an offset is unusable: converting it would be a guess."""
    await redis.hset(
        PROGRESS_KEY,
        mapping={"run_id": "1", "status": "running", "started_at": "2026-08-09T10:00:00"},
    )

    assert await RedisProgressStore(redis).read() is None


@pytest.mark.parametrize(
    ("field", "value"),
    [("names_seen", b"\xd0\xbc\xd0\xbd\xd0\xbe\xd0\xb3\xd0\xbe"), ("files_saved", b"3.5")],
)
async def test_a_broken_counter_rejects_the_whole_record(redis, field: str, value: bytes) -> None:
    """Falling back to zero here would show a zero where the database has a number."""
    await redis.hset(PROGRESS_KEY, mapping=valid_hash() | {field: value})

    assert await RedisProgressStore(redis).read() is None


async def test_a_broken_retry_deadline_rejects_the_record(redis) -> None:
    """A pause the page cannot count down to is not a pause worth showing."""
    await redis.hset(
        PROGRESS_KEY, mapping=valid_hash() | {"retry_at": b"2026-08-09", "retry_reason": b"429"}
    )

    assert await RedisProgressStore(redis).read() is None


async def test_invalid_utf8_in_a_value_reads_as_none(redis) -> None:
    """Redis returns bytes, and nothing guarantees they decode (§ 5)."""
    await redis.hset(PROGRESS_KEY, mapping=valid_hash() | {"status": b"\xff\xfe"})

    assert await RedisProgressStore(redis).read() is None


async def test_invalid_utf8_in_a_key_reads_as_none(redis) -> None:
    await redis.hset(PROGRESS_KEY, mapping=valid_hash() | {b"\xff": b"1"})

    assert await RedisProgressStore(redis).read() is None


class UnreachableRedis:
    """Every command fails, as it would with the server gone."""

    def pipeline(self, transaction: bool = True) -> "UnreachableRedis":
        return self

    async def __aenter__(self) -> "UnreachableRedis":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    def delete(self, *args: object) -> None:
        return None

    def hset(self, *args: object, **kwargs: object) -> None:
        return None

    def expire(self, *args: object) -> None:
        return None

    async def execute(self) -> None:
        raise RedisError("соединение с Redis потеряно")

    async def hgetall(self, *args: object) -> None:
        raise RedisError("соединение с Redis потеряно")


async def test_publishing_progress_never_raises() -> None:
    """Best effort by contract: a run must not die because its counters got lost."""
    await RedisProgressStore(UnreachableRedis()).publish(sample())


async def test_reading_progress_never_raises() -> None:
    assert await RedisProgressStore(UnreachableRedis()).read() is None
