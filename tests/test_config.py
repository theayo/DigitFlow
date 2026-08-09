"""Configuration bounds, checked when Settings loads (§ 11).

These values used to be trusted and only bite much later: a zero interval turns
the pacing into a flood that earns a 30-minute ban, and a heartbeat that is not
shorter than the lock TTL lets the lock expire under a working run. Both used to
surface far away from their cause — inside Redis, or as a lost lock halfway
through a download — so they are refused at load time instead.
"""

import pytest
from pydantic import ValidationError

from app.config import Settings

# Everything without a default. The rest comes from the class, which is exactly
# what the "defaults are valid" test below is about.
REQUIRED = {
    "database_url": "postgresql+asyncpg://app:app@postgres:5432/files",
    "redis_url": "redis://redis:6379/0",
    "rabbitmq_url": "amqp://guest:guest@rabbitmq:5672//",
    "external_api_base_url": "http://external.test",
    "candidate_id": "test-candidate",
}


def build(**overrides: object) -> Settings:
    return Settings(**{**REQUIRED, **overrides})


def test_defaults_are_valid() -> None:
    settings = build()

    assert settings.run_lock_heartbeat_s < settings.run_lock_ttl_s
    assert settings.external_min_interval_max_ms >= settings.external_min_interval_ms


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("external_timeout_s", 0),
        ("external_timeout_s", -1),
        ("external_min_interval_ms", 0),
        ("external_min_interval_ms", -1),
        ("external_min_interval_max_ms", 0),
        ("max_retry_wait_s", 0),
        ("network_max_attempts", 0),
        ("network_backoff_base_s", -0.5),
        ("single_404_attempts", 0),
        ("max_stale_iterations", 0),
        ("zip_max_entry_bytes", 0),
        ("run_lock_ttl_s", 0),
        ("run_lock_heartbeat_s", 0),
        ("run_pending_grace_s", 0),
        ("run_starting_grace_s", 0),
        ("run_events_tail", 0),
        ("files_page_size_max", 0),
        ("stats_page_size_max", 0),
        ("max_file_ids", 0),
    ],
)
def test_values_at_or_below_the_bound_are_rejected(field: str, value: float) -> None:
    with pytest.raises(ValidationError) as error:
        build(**{field: value})

    assert field in str(error.value)


def test_zero_backoff_is_allowed() -> None:
    """Explicitly a choice, not an oversight: it means "retry immediately"."""
    assert build(network_backoff_base_s=0).network_backoff_base_s == 0


# --- relationships between fields -------------------------------------------


@pytest.mark.parametrize("heartbeat", [60, 61])
def test_a_heartbeat_that_does_not_beat_in_time_is_rejected(heartbeat: int) -> None:
    """Equal is already too late: renewal has to happen strictly before expiry."""
    with pytest.raises(ValidationError) as error:
        build(run_lock_ttl_s=60, run_lock_heartbeat_s=heartbeat)

    assert "RUN_LOCK_HEARTBEAT_S" in str(error.value)


def test_a_heartbeat_shorter_than_the_ttl_is_accepted() -> None:
    assert build(run_lock_ttl_s=60, run_lock_heartbeat_s=59).run_lock_heartbeat_s == 59


def test_an_interval_ceiling_below_the_floor_is_rejected() -> None:
    with pytest.raises(ValidationError) as error:
        build(external_min_interval_ms=5000, external_min_interval_max_ms=1000)

    assert "EXTERNAL_MIN_INTERVAL_MAX_MS" in str(error.value)


def test_an_interval_ceiling_equal_to_the_floor_is_accepted() -> None:
    """A fixed pace with no growth after a 429 is a legitimate configuration."""
    settings = build(external_min_interval_ms=1000, external_min_interval_max_ms=1000)

    assert settings.external_min_interval_max_ms == 1000
