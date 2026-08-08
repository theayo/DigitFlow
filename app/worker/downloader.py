"""The download loop: names -> archive -> database -> confirmation.

Ordering is the load-bearing part. A file is confirmed to the external API only
after it is committed here, because /names hands out everything that is not
confirmed, and /downloaded rejects names the catalog does not know. A file we
cannot save and cannot confirm would therefore come back forever, which is why
every unrecoverable problem ends the run instead of skipping the file.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import timedelta
from typing import TypeVar

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import CONTENT_LENGTH, DownloadRun, File, RunEvent
from app.time import utc_now
from app.worker.archive import ParsedFile, content_hash, digit_counts, parse_archive
from app.worker.client import MAX_NAMES_PER_DOWNLOAD, FilesApiClient
from app.worker.errors import (
    ArchiveError,
    LockLost,
    NotFoundError,
    RetryAfterError,
    RunFailed,
    UnprocessableError,
)
from app.worker.lock import RunLock
from app.worker.progress import Progress, publish

T = TypeVar("T")


def chunked(items: Sequence[str], size: int) -> Iterator[list[str]]:
    """Split names into batches the download endpoint will accept."""
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


class DownloadRunner:
    """Runs a single download run from start to terminal status."""

    def __init__(
        self,
        run_id: int,
        settings: Settings,
        client: FilesApiClient,
        lock: RunLock,
        sessionmaker: async_sessionmaker[AsyncSession],
        redis,
    ) -> None:
        self._run_id = run_id
        self._settings = settings
        self._client = client
        self._lock = lock
        self._sessionmaker = sessionmaker
        self._redis = redis

        self._names_seen = 0
        self._files_saved = 0
        self._started_at = utc_now()
        self._status = "running"

    # --- public entry point -------------------------------------------------

    async def execute(self) -> str:
        """Run the loop and return the terminal status."""
        try:
            await self._begin()
            await self._loop()
        except LockLost as exc:
            await self._finish("failed", f"работа прекращена: {exc}")
            return "failed"
        except RunFailed as exc:
            await self._finish("failed", str(exc))
            return "failed"
        except Exception as exc:  # noqa: BLE001 — any escape must still close the run
            await self._finish("failed", f"непредвиденная ошибка: {exc}")
            raise
        await self._finish("done", None)
        return "done"

    # --- main loop ----------------------------------------------------------

    async def _loop(self) -> None:
        stale_iterations = 0

        while True:
            names = await self._await_pauses(self._client.get_names, "получение имён")
            if not names:
                await self._event("info", "каталог скачан полностью: имена закончились")
                return

            self._names_seen += len(names)
            await self._event("info", f"получено {len(names)} названий файлов")
            await self._publish()

            marked_total = 0
            for chunk in chunked(names, MAX_NAMES_PER_DOWNLOAD):
                marked_total += await self._process_chunk(chunk)

            # Safety net for cases the rules above did not anticipate: names keep
            # arriving but nothing ever gets confirmed.
            if marked_total == 0:
                stale_iterations += 1
                await self._event(
                    "warning",
                    f"итерация без прогресса ({stale_iterations} подряд): "
                    "ни один файл не был подтверждён",
                )
                if stale_iterations >= self._settings.max_stale_iterations:
                    raise RunFailed(
                        f"{stale_iterations} итераций подряд без единого подтверждённого "
                        "файла — процесс не сходится"
                    )
            else:
                stale_iterations = 0

    async def _process_chunk(self, chunk: list[str], attempt: int = 1) -> int:
        """Download, validate, save and confirm one chunk. Returns files confirmed."""
        try:
            payload = await self._await_pauses(
                lambda: self._client.download(chunk), f"скачивание {len(chunk)} файлов"
            )
            parsed = parse_archive(payload, chunk, self._settings.zip_max_entry_bytes)
        except (NotFoundError, ArchiveError) as exc:
            return await self._isolate(chunk, exc, attempt)

        saved = await self._save(parsed)

        # The single most important ownership check in the loop: confirming a file
        # removes it from /names forever, so it must never happen on behalf of a run
        # that no longer holds the slot.
        await self._lock.ensure_owned()
        marked_now, already = await self._await_pauses(
            lambda: self._client.mark_downloaded([file.name for file in saved]),
            "отметка скачанных",
        )
        self._files_saved += len(saved)
        await self._event(
            "info",
            f"сохранено и отмечено {len(saved)} файлов "
            f"(новых на той стороне {marked_now}, уже отмеченных {already})",
        )
        await self._publish()
        # Only newly marked files count as progress: already_marked means the file
        # was confirmed before yet /names handed it out again, which is exactly the
        # loop the stale-iteration guard exists to catch.
        return marked_now

    async def _isolate(self, chunk: list[str], exc: Exception, attempt: int) -> int:
        """Find the offending name by requesting the chunk one name at a time."""
        if len(chunk) > 1:
            await self._event(
                "warning",
                f"чанк из {len(chunk)} имён отклонён ({exc}); разбиваю на одиночные запросы",
            )
            confirmed = 0
            for name in chunk:
                confirmed += await self._process_chunk([name], attempt=1)
            return confirmed

        name = chunk[0]
        if attempt < self._settings.single_404_attempts:
            await self._event(
                "warning",
                f"файл {name} отклонён ({exc}), попытка {attempt} из "
                f"{self._settings.single_404_attempts}",
            )
            await asyncio.sleep(self._settings.network_backoff_base_s)
            return await self._process_chunk([name], attempt=attempt + 1)

        raise RunFailed(
            f"файл {name} не удалось получить за {attempt} попыток: {exc}. "
            "Продолжать нельзя: этот файл невозможно ни скачать, ни отметить, "
            "и он будет возвращаться из /names бесконечно"
        )

    # --- persistence --------------------------------------------------------

    async def _save(self, parsed: Sequence[ParsedFile]) -> list[ParsedFile]:
        """Insert files, verifying anything that already exists.

        Returns the files that may be confirmed to the external API — nothing
        else is ever sent to /downloaded.
        """
        confirmed: list[ParsedFile] = []

        async with self._sessionmaker() as session:
            for file in parsed:
                values = {
                    "name": file.name,
                    "content": file.content,
                    "content_hash": file.content_hash,
                    "downloaded_at": utc_now(),
                    "run_id": self._run_id,
                    **{f"d{i}": file.digit_counts[i] for i in range(10)},
                }
                statement = (
                    pg_insert(File)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=["name"])
                    .returning(File.id)
                )
                inserted = (await session.execute(statement)).scalar_one_or_none()

                if inserted is None:
                    # DO NOTHING swallows both a match and a divergence, so the
                    # existing row has to be read and compared explicitly.
                    await self._verify_existing(session, file)

                confirmed.append(file)

            await self._lock.ensure_owned()
            await session.commit()

        return confirmed

    async def _verify_existing(self, session: AsyncSession, file: ParsedFile) -> None:
        existing = (
            await session.execute(select(File).where(File.name == file.name))
        ).scalar_one_or_none()

        if existing is None:
            raise RunFailed(f"файл {file.name} не вставился и не найден — конфликт неразрешим")

        stored = existing.content.rstrip()
        recomputed = digit_counts(stored) if len(stored) == CONTENT_LENGTH else ()
        stored_counts = tuple(getattr(existing, f"d{i}") for i in range(10))

        if len(stored) != CONTENT_LENGTH or content_hash(stored) != existing.content_hash:
            raise RunFailed(
                f"существующая запись {file.name} повреждена: хеш и содержимое "
                "не сходятся. Файл не подтверждён"
            )

        # The statistics columns are what the calculations page reads; a row whose
        # counters disagree with its own content is corrupt even though the hash,
        # the length and the sum-500 constraint all look fine.
        if recomputed != stored_counts:
            raise RunFailed(
                f"существующая запись {file.name} повреждена: статистика цифр "
                f"не соответствует содержимому — в базе {stored_counts}, "
                f"по содержимому {recomputed}. Файл не подтверждён"
            )

        if existing.content_hash != file.content_hash:
            raise RunFailed(
                f"содержимое {file.name} разошлось: в базе {existing.content_hash}, "
                f"скачано {file.content_hash}. Файл не подтверждён — молча выбирать "
                "победителя нельзя"
            )

    # --- pauses -------------------------------------------------------------

    async def _await_pauses(self, operation: Callable[[], Awaitable[T]], description: str) -> T:
        """Run an operation, sitting through Retry-After pauses.

        The wait happens here rather than inside the client so that it becomes
        visible on the download page.
        """
        while True:
            # Checked before every outgoing request, and therefore also after a
            # Retry-After pause, before the operation is attempted again.
            await self._lock.ensure_owned()
            try:
                return await operation()
            except RetryAfterError as exc:
                delay = exc.retry_after
                if delay is None:
                    # 429 without the header: fall back to our own pacing.
                    delay = self._settings.network_backoff_base_s * 2
                if delay > self._settings.max_retry_wait_s:
                    raise RunFailed(
                        f"требуемая пауза {delay:.0f} с превышает предел "
                        f"{self._settings.max_retry_wait_s} с ({description})"
                    ) from exc

                reason = f"{exc.status_code} от {exc.endpoint}, пауза {delay:.0f} с ({description})"
                await self._enter_waiting(reason, delay)
                await asyncio.sleep(delay)
                await self._leave_waiting()
            except UnprocessableError as exc:
                raise RunFailed(f"наш запрос отвергнут как некорректный: {exc}") from exc

    async def _enter_waiting(self, reason: str, delay: float) -> None:
        self._status = "waiting_retry"
        await self._event("warning", reason)
        await self._set_status("waiting_retry")
        await self._publish(retry_at=utc_now() + timedelta(seconds=delay), retry_reason=reason)

    async def _leave_waiting(self) -> None:
        self._status = "running"
        await self._set_status("running")
        await self._publish()

    # --- run bookkeeping ----------------------------------------------------

    async def _begin(self) -> None:
        async with self._sessionmaker() as session:
            run = await session.get(DownloadRun, self._run_id)
            if run is None:
                raise RunFailed(f"ран {self._run_id} не найден в базе")
            run.status = "running"
            run.started_at = self._started_at
            await session.commit()

        await self._event("info", "процесс скачивания запущен")
        await self._publish()

    async def _finish(self, status: str, error: str | None) -> None:
        self._status = status
        async with self._sessionmaker() as session:
            run = await session.get(DownloadRun, self._run_id)
            if run is not None:
                run.status = status
                run.finished_at = utc_now()
                run.names_seen = self._names_seen
                run.files_saved = self._files_saved
                run.error = error
                await session.commit()

        level = "error" if status == "failed" else "info"
        message = error if error else f"процесс завершён: скачано {self._files_saved} файлов"
        await self._event(level, message)
        await self._publish()

    async def _set_status(self, status: str) -> None:
        async with self._sessionmaker() as session:
            run = await session.get(DownloadRun, self._run_id)
            if run is not None:
                run.status = status
                await session.commit()

    async def _event(self, level: str, message: str) -> None:
        async with self._sessionmaker() as session:
            session.add(RunEvent(run_id=self._run_id, level=level, message=message, ts=utc_now()))
            await session.commit()

    async def _publish(self, retry_at=None, retry_reason=None) -> None:
        # Progress is a convenience for the UI. If Redis is gone the run is already
        # doomed by the ownership check, and that failure has to reach the database
        # rather than being replaced by this one.
        with contextlib.suppress(RedisError):
            await publish(
                self._redis,
                Progress(
                    run_id=self._run_id,
                    status=self._status,
                    started_at=self._started_at,
                    names_seen=self._names_seen,
                    files_saved=self._files_saved,
                    retry_at=retry_at,
                    retry_reason=retry_reason,
                ),
            )
