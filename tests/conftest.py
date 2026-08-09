"""Test isolation and shared fixtures.

The suite creates, mutates and truncates data, so it must never be pointed at the
working storage. Before anything else is imported, the environment is redirected
to `TEST_DATABASE_URL` and `TEST_REDIS_URL`, and the run aborts outright if either
still matches its working counterpart.
"""

import asyncio
import os
from collections.abc import AsyncIterator, Iterator

import asyncpg
import pytest
import redis.asyncio as aioredis
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from tests.storage_targets import postgres_target, redis_target

# --- isolation, performed at import time ------------------------------------
#
# Compared as normalised targets rather than as strings: the same database can be
# spelled several ways, and a spelling difference must not be mistaken for
# isolation. Raised here, before any engine, migration, TRUNCATE or Redis cleanup.

_working = get_settings()

if postgres_target(_working.test_database_url) == postgres_target(_working.database_url):
    raise pytest.UsageError(
        "TEST_DATABASE_URL указывает на ту же базу, что и DATABASE_URL "
        f"({postgres_target(_working.database_url)}) — тесты отказываются работать "
        "с рабочими данными. Задайте отдельную базу, например files_test."
    )

if redis_target(_working.test_redis_url) == redis_target(_working.redis_url):
    raise pytest.UsageError(
        "TEST_REDIS_URL указывает на тот же Redis, что и REDIS_URL "
        f"({redis_target(_working.redis_url)}) — тесты отказываются работать "
        "с рабочими ключами. Задайте отдельный номер базы, например /1."
    )

# Captured before the redirect below: afterwards `settings.database_url` is the
# test database, and the working values are no longer reachable from settings.
WORKING_DATABASE_URL = _working.database_url
WORKING_REDIS_URL = _working.redis_url

TEST_DATABASE_URL = _working.test_database_url
TEST_REDIS_URL = _working.test_redis_url

os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["REDIS_URL"] = TEST_REDIS_URL
get_settings.cache_clear()

# Imported only after the redirect: these modules resolve settings lazily, but the
# import order should not be something a reader has to verify.
from httpx import ASGITransport, AsyncClient  # noqa: E402

from app.db import get_engine, get_sessionmaker  # noqa: E402
from app.dependencies import get_task_publisher  # noqa: E402
from app.main import app  # noqa: E402
from app.progress import PROGRESS_KEY  # noqa: E402
from app.worker.celery_app import celery_app  # noqa: E402
from app.worker.lock import LOCK_KEY  # noqa: E402
from app.worker.ratelimit import INTERVAL_KEY, NEXT_ALLOWED_KEY  # noqa: E402

WORKER_KEYS = (PROGRESS_KEY, LOCK_KEY, NEXT_ALLOWED_KEY, INTERVAL_KEY)
TABLES = ("run_event", "file", "download_run")


def _create_database_if_missing() -> None:
    """Create the test database, connecting to the server's default one."""
    url = make_url(TEST_DATABASE_URL)

    async def run() -> None:
        connection = await asyncpg.connect(
            host=url.host,
            port=url.port,
            user=url.username,
            password=url.password,
            database="postgres",
        )
        try:
            exists = await connection.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", url.database
            )
            if not exists:
                # CREATE DATABASE cannot run inside a transaction block.
                await connection.execute(f'CREATE DATABASE "{url.database}"')
        finally:
            await connection.close()

    asyncio.run(run())


def _migrate() -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)
    command.upgrade(config, "head")


@pytest.fixture(scope="session", autouse=True)
async def prepared_database() -> AsyncIterator[None]:
    """Create and migrate the test schema, then wipe it and close the pool."""
    # Both helpers open their own event loop, so they run in a worker thread.
    await asyncio.to_thread(_create_database_if_missing)
    await asyncio.to_thread(_migrate)

    await _truncate()
    try:
        yield
    finally:
        await _truncate()
        await get_engine().dispose()


async def _truncate() -> None:
    async with get_engine().begin() as connection:
        await connection.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))


@pytest.fixture
async def clean_db() -> AsyncIterator[None]:
    """Empty tables for the duration of one test.

    Requested by tests that read "the latest run" or count files, where rows left
    behind by an earlier test would change the answer.
    """
    await _truncate()
    try:
        yield
    finally:
        await _truncate()


@pytest.fixture
async def api() -> AsyncIterator[AsyncClient]:
    """HTTP client speaking to the application in-process."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
async def redis() -> AsyncIterator[aioredis.Redis]:
    """Redis client on the dedicated test database, cleared around each test."""
    client = aioredis.from_url(TEST_REDIS_URL)
    await client.delete(*WORKER_KEYS)
    try:
        yield client
    finally:
        await client.delete(*WORKER_KEYS)
        await client.aclose()


class RecordingPublisher:
    """A task publisher that remembers instead of publishing."""

    def __init__(self) -> None:
        self.published: list[int] = []

    async def __call__(self, run_id: int) -> None:
        self.published.append(run_id)


@pytest.fixture(autouse=True)
def published_tasks(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    """Replace the task publisher for every test. Autouse, and deliberately not opt-in.

    A real publish would be picked up by the worker container running alongside
    the suite, and that worker downloads from the live external API. The rule
    "tests never touch the external API" is enforced here rather than trusted to
    every test that starts a run.

    Two layers, because the port alone only covers what goes through `Depends`:
    the API gets a recording publisher, and the broker call underneath is made to
    fail loudly, so any path that bypasses the port fails the test instead of
    reaching RabbitMQ.
    """
    publisher = RecordingPublisher()
    app.dependency_overrides[get_task_publisher] = lambda: publisher

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("тест попытался опубликовать задачу в настоящий брокер")

    monkeypatch.setattr(celery_app, "send_task", forbidden)
    try:
        yield publisher.published
    finally:
        app.dependency_overrides.pop(get_task_publisher, None)


@pytest.fixture
def sessionmaker() -> async_sessionmaker[AsyncSession]:
    return get_sessionmaker()


@pytest.fixture(scope="session")
def storage_urls() -> dict[str, str]:
    """Working and test URLs, as a fixture rather than an import.

    Importing this module from a test would execute it a second time, after the
    redirect below has already happened, and the isolation check would fire
    against its own result.
    """
    return {
        "working_database": WORKING_DATABASE_URL,
        "working_redis": WORKING_REDIS_URL,
        "test_database": TEST_DATABASE_URL,
        "test_redis": TEST_REDIS_URL,
    }
