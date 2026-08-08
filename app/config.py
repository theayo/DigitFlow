"""Application settings. Every value comes from the environment."""

from functools import lru_cache

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
    external_timeout_s: float = 30.0

    # --- request interval ---
    external_min_interval_ms: int = 1000
    external_min_interval_max_ms: int = 15000
    max_retry_wait_s: int = 1900
    network_max_attempts: int = 5
    network_backoff_base_s: float = 1.0

    # --- inf cycles ---
    single_404_attempts: int = 2
    max_stale_iterations: int = 3

    # --- validation zip ---
    zip_max_entry_bytes: int = 65536

    # --- lock run ---
    run_lock_ttl_s: int = 60
    run_lock_heartbeat_s: int = 20

    # --- file limits ---
    files_page_size_max: int = 200
    stats_page_size_max: int = 200
    max_file_ids: int = 1000

    # --- presentation ---
    # Used only when rendering timestamps for the UI. Storage and sorting stay UTC.
    display_timezone: str = "Asia/Novosibirsk"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
