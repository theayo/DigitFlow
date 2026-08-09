"""SQLAlchemy engine and sessions, plus the Redis client shared by the API.

The Celery task builds its own Redis client: every invocation runs on a fresh
event loop, and a cached connection pool would outlive the loop it was created
on. The API process has a single loop, so one pool serves every request.
"""

from collections.abc import AsyncIterator
from functools import lru_cache

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings


@lru_cache
def get_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url, pool_pre_ping=True)


@lru_cache
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


@lru_cache
def get_redis() -> aioredis.Redis:
    """Redis client shared by the whole API process."""
    return aioredis.from_url(get_settings().redis_url)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: one session per request."""
    async with get_sessionmaker()() as session:
        yield session
