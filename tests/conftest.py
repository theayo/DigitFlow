"""Shared test fixtures."""

from collections.abc import AsyncIterator

import pytest

from app.db import get_engine


@pytest.fixture(scope="session", autouse=True)
async def dispose_engine() -> AsyncIterator[None]:
    """Close the connection pool before the session event loop shuts down."""
    yield
    await get_engine().dispose()
