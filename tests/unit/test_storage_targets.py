"""Unit tests for connection-target normalisation.

These guard the guard: if two spellings of the same database compare as different,
the isolation check in conftest passes and the suite is free to truncate the
working data.
"""

from tests.storage_targets import postgres_target, redis_target

WORKING_DB = "postgresql+asyncpg://app:app@postgres:5432/files"
WORKING_REDIS = "redis://redis:6379/0"


# --- PostgreSQL -------------------------------------------------------------


def test_implicit_and_explicit_default_port_are_the_same_target() -> None:
    assert postgres_target("postgresql+asyncpg://app:app@postgres/files") == postgres_target(
        WORKING_DB
    )


def test_credentials_do_not_make_another_database() -> None:
    """Connecting as another user does not turn it into different data."""
    assert postgres_target("postgresql+asyncpg://other:secret@postgres:5432/files") == (
        postgres_target(WORKING_DB)
    )


def test_host_case_is_ignored() -> None:
    assert postgres_target("postgresql+asyncpg://app:app@POSTGRES:5432/files") == postgres_target(
        WORKING_DB
    )


def test_different_database_name_is_another_target() -> None:
    assert postgres_target("postgresql+asyncpg://app:app@postgres:5432/files_test") != (
        postgres_target(WORKING_DB)
    )


def test_different_host_is_another_target() -> None:
    assert postgres_target("postgresql+asyncpg://app:app@pg-test:5432/files") != postgres_target(
        WORKING_DB
    )


def test_different_port_is_another_target() -> None:
    assert postgres_target("postgresql+asyncpg://app:app@postgres:5433/files") != postgres_target(
        WORKING_DB
    )


def test_driver_does_not_change_the_target() -> None:
    assert postgres_target("postgresql://app:app@postgres:5432/files") == postgres_target(
        WORKING_DB
    )


# --- Redis ------------------------------------------------------------------


def test_missing_redis_index_means_zero() -> None:
    assert redis_target("redis://redis:6379") == redis_target(WORKING_REDIS)


def test_missing_redis_index_and_port_means_defaults() -> None:
    assert redis_target("redis://redis") == redis_target(WORKING_REDIS)


def test_redis_credentials_do_not_make_another_target() -> None:
    assert redis_target("redis://:secret@redis:6379/0") == redis_target(WORKING_REDIS)


def test_different_redis_index_is_another_target() -> None:
    assert redis_target("redis://redis:6379/1") != redis_target(WORKING_REDIS)


def test_different_redis_host_is_another_target() -> None:
    assert redis_target("redis://redis-test:6379/0") != redis_target(WORKING_REDIS)


def test_different_redis_port_is_another_target() -> None:
    assert redis_target("redis://redis:6380/0") != redis_target(WORKING_REDIS)


# --- the configuration actually in use --------------------------------------


def test_configured_test_storage_differs_from_working_storage(
    storage_urls: dict[str, str],
) -> None:
    """The check conftest performs, asserted explicitly for the current .env.

    Uses the values captured before conftest redirected the environment: by the
    time a test runs, settings already point at the test storage, so comparing
    settings with settings would prove nothing.
    """
    assert postgres_target(storage_urls["test_database"]) != postgres_target(
        storage_urls["working_database"]
    )
    assert redis_target(storage_urls["test_redis"]) != redis_target(storage_urls["working_redis"])
