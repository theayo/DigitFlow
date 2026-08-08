"""Celery configuration: RabbitMQ broker, Redis result backend."""

from celery import Celery

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "downloader",
    broker=settings.rabbitmq_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # A run can last for hours and sit through Retry-After pauses: the task must not
    # be acknowledged before it finishes, and a worker crash must not lose it.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    imports=("app.worker.tasks",),
)
