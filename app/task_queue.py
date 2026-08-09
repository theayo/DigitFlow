"""Publish download tasks to Celery without blocking the API event loop."""

import asyncio
from collections.abc import Awaitable, Callable

from app.worker.celery_app import celery_app

DOWNLOAD_TASK_NAME = "download.run"

TaskPublisher = Callable[[int], Awaitable[None]]


async def publish_download(run_id: int) -> None:
    """Hand a committed run to Celery, propagating ambiguous delivery errors."""
    await asyncio.to_thread(celery_app.send_task, DOWNLOAD_TASK_NAME, args=[run_id])
