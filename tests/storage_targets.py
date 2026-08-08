"""Normalised connection targets, used to prove the test storage is not the working one.

Comparing URLs as plain strings is not enough: `postgres:5432/files` and
`postgres/files` are the same database written two ways, and a mismatch that slips
through here means the suite is free to TRUNCATE production data.

Credentials are deliberately excluded from the target. Connecting as another user
does not make it a different database.
"""

from typing import NamedTuple
from urllib.parse import urlsplit

from sqlalchemy.engine import make_url

DEFAULT_POSTGRES_PORT = 5432
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0


class PostgresTarget(NamedTuple):
    host: str
    port: int
    database: str


class RedisTarget(NamedTuple):
    host: str
    port: int
    db: int


def postgres_target(url: str) -> PostgresTarget:
    """Reduce a PostgreSQL URL to what actually identifies the data."""
    parsed = make_url(url)
    return PostgresTarget(
        host=(parsed.host or "localhost").lower(),
        port=parsed.port or DEFAULT_POSTGRES_PORT,
        database=parsed.database or "",
    )


def redis_target(url: str) -> RedisTarget:
    """Reduce a Redis URL to host, port and database index."""
    parsed = urlsplit(url)
    path = parsed.path.strip("/")
    return RedisTarget(
        host=(parsed.hostname or "localhost").lower(),
        port=parsed.port or DEFAULT_REDIS_PORT,
        db=int(path) if path else DEFAULT_REDIS_DB,
    )
