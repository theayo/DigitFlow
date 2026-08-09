"""Application settings. Every value comes from the environment.

The bounds below are not decoration. A zero interval turns the pacing into a
flood and earns a 30-minute ban; a heartbeat that is not shorter than the lock
TTL means the lock expires under a working run and the slot is handed to someone
else mid-download. Both used to surface far from their cause — inside Redis, or
as a lost lock halfway through a run — so they are refused at load time instead,
which stops the API and the worker with a plain `ValidationError`.
"""

from functools import lru_cache
from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- infra ---
    database_url: str
    redis_url: str
    rabbitmq_url: str

    # --- external API ---
    external_api_base_url: str
    candidate_id: str
    external_timeout_s: float = Field(default=30.0, gt=0)

    # --- request interval ---
    external_min_interval_ms: int = Field(default=1000, gt=0)
    external_min_interval_max_ms: int = Field(default=15000, gt=0)
    max_retry_wait_s: int = Field(default=1900, gt=0)
    network_max_attempts: int = Field(default=5, ge=1)
    # Zero is allowed and means "retry immediately": the backoff exists for
    # network errors, and a deployment that would rather not wait at all is
    # making a defensible choice. A negative delay is not a choice, it is a bug.
    network_backoff_base_s: float = Field(default=1.0, ge=0)

    # --- inf cycles ---
    single_404_attempts: int = Field(default=2, ge=1)
    max_stale_iterations: int = Field(default=3, ge=1)

    # --- validation zip ---
    zip_max_entry_bytes: int = Field(default=65536, gt=0)

    # --- lock run ---
    run_lock_ttl_s: int = Field(default=60, gt=0)
    run_lock_heartbeat_s: int = Field(default=20, gt=0)
    # How long a queued run may stay `pending` before it is treated as abandoned.
    # A pending run holds no lock yet, so it cannot be reaped by ownership alone.
    run_pending_grace_s: int = Field(default=300, gt=0)
    # Same idea for `starting`, but far shorter: the run is already in a worker's
    # hands and only has to take the lock. Long enough to cover that, short enough
    # that a worker dying mid-startup does not block the service.
    run_starting_grace_s: int = Field(default=30, gt=0)

    # --- run reporting ---
    run_events_tail: int = Field(default=100, ge=1)

    # --- file limits ---
    files_page_size_max: int = Field(default=200, ge=1)
    stats_page_size_max: int = Field(default=200, ge=1)
    max_file_ids: int = Field(default=1000, ge=1)

    # --- presentation ---
    # Used only when rendering timestamps for the UI. Storage and sorting stay UTC.
    display_timezone: str = "Asia/Novosibirsk"

    # --- tests ---
    # Test Storage. The test database is created and migrated automatically;
    test_database_url: str = "postgresql+asyncpg://app:app@postgres:5432/files_test"
    test_redis_url: str = "redis://redis:6379/1"

    @model_validator(mode="after")
    def _check_invariants(self) -> Self:
        if self.run_lock_heartbeat_s >= self.run_lock_ttl_s:
            raise ValueError(
                "RUN_LOCK_HEARTBEAT_S должен быть строго меньше RUN_LOCK_TTL_S "
                f"(сейчас {self.run_lock_heartbeat_s} и {self.run_lock_ttl_s}): "
                "иначе lock протухает раньше, чем его успевают продлить, "
                "и слот уходит другому воркеру посреди выкачки"
            )
        if self.external_min_interval_max_ms < self.external_min_interval_ms:
            raise ValueError(
                "EXTERNAL_MIN_INTERVAL_MAX_MS не может быть меньше EXTERNAL_MIN_INTERVAL_MS "
                f"(сейчас {self.external_min_interval_max_ms} и {self.external_min_interval_ms}): "
                "потолок роста интервала оказался ниже стартового значения"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
