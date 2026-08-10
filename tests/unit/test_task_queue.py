"""Publishing a download task to Celery.

No broker and no database: the adapter is small, and what matters about it is
that the blocking `send_task` leaves the event loop and that a failed delivery
reaches the caller instead of being swallowed.
"""

import threading

from app.task_queue import DOWNLOAD_TASK_NAME, publish_download


class FakeCelery:
    """Records what would have been published."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list, int]] = []

    def send_task(self, name: str, args: list | None = None, **kwargs: object) -> object:
        self.calls.append((name, list(args or []), threading.get_ident()))
        return object()


async def test_publisher_hands_the_run_to_celery_off_the_event_loop(monkeypatch) -> None:
    """send_task blocks on a socket, so it must not run on the loop thread."""
    celery = FakeCelery()

    monkeypatch.setattr("app.task_queue.celery_app.send_task", celery.send_task)
    await publish_download(17)

    assert [(name, args) for name, args, _ in celery.calls] == [(DOWNLOAD_TASK_NAME, [17])]
    assert celery.calls[0][2] != threading.get_ident()


async def test_publisher_propagates_a_delivery_failure(monkeypatch) -> None:
    """The caller has to learn that delivery is in doubt, not be told it succeeded."""

    class BrokenCelery:
        def send_task(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("брокер не подтвердил приём")

    monkeypatch.setattr("app.task_queue.celery_app.send_task", BrokenCelery().send_task)
    try:
        await publish_download(1)
    except RuntimeError as exc:
        assert "брокер" in str(exc)
    else:
        raise AssertionError("ошибка публикации должна дойти до вызывающего")
