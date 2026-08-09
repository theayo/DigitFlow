"""FastAPI dependency factories for external infrastructure."""

from app.db import get_redis
from app.progress import ProgressStore, RedisProgressStore
from app.services.runs import RunSlot
from app.task_queue import TaskPublisher, publish_download
from app.worker.lock import RedisRunSlot


def get_task_publisher() -> TaskPublisher:
    return publish_download


def get_progress_store() -> ProgressStore:
    return RedisProgressStore(get_redis())


def get_run_slot() -> RunSlot:
    return RedisRunSlot(get_redis())
