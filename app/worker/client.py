"""HTTP client for the external file API. The only module that talks to it.

Two kinds of waiting are deliberately kept apart. Network failures and 5xx are
retried here, invisibly to the caller. A 429 or 403 is raised as
`RetryAfterError` instead: the pause must be visible in the UI, so the caller
decides when to sleep and reports it.
"""

import asyncio
import random
from collections.abc import Sequence
from email.utils import parsedate_to_datetime

import httpx

from app.config import Settings
from app.time import utc_now
from app.worker.errors import (
    ExternalServerError,
    NetworkExhausted,
    NotFoundError,
    RetryAfterError,
    UnprocessableError,
)
from app.worker.ratelimit import RateLimiter

NAMES_PATH = "/api/files/names"
DOWNLOAD_PATH = "/api/files/download"
DOWNLOADED_PATH = "/api/files/downloaded"

# The external API accepts at most three names per download request.
MAX_NAMES_PER_DOWNLOAD = 3


def parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header given either as seconds or as an HTTP date."""
    if value is None:
        return None

    raw = value.strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass

    try:
        deadline = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if deadline is None:
        return None
    if deadline.tzinfo is None:
        return None
    return max(0.0, (deadline - utc_now()).total_seconds())


def backoff_delay(attempt: int, base_s: float, jitter: float) -> float:
    """Exponential backoff with equal jitter, for network errors and 5xx only.

    `jitter` is a value in [0, 1); passing it in keeps the function pure and
    testable.
    """
    ceiling = base_s * (2 ** (attempt - 1))
    return ceiling / 2 + ceiling / 2 * jitter


class FilesApiClient:
    """Thin wrapper over the three endpoints of the external API."""

    def __init__(
        self,
        settings: Settings,
        limiter: RateLimiter,
        http: httpx.AsyncClient,
    ) -> None:
        self._settings = settings
        self._limiter = limiter
        self._http = http

    @property
    def _candidate_headers(self) -> dict[str, str]:
        # Only /names and /downloaded accept this header; /download has no such
        # parameter in the specification.
        return {"X-Candidate-Id": self._settings.candidate_id}

    async def get_names(self) -> list[str]:
        response = await self._request("GET", NAMES_PATH, headers=self._candidate_headers)
        payload = response.json()
        return list(payload.get("file_names", []))

    async def download(self, names: Sequence[str]) -> bytes:
        if not 1 <= len(names) <= MAX_NAMES_PER_DOWNLOAD:
            raise ValueError(
                f"download accepts 1..{MAX_NAMES_PER_DOWNLOAD} names, got {len(names)}"
            )
        response = await self._request("POST", DOWNLOAD_PATH, json={"file_names": list(names)})
        return response.content

    async def mark_downloaded(self, names: Sequence[str]) -> tuple[int, int]:
        response = await self._request(
            "POST",
            DOWNLOADED_PATH,
            json={"file_names": list(names)},
            headers=self._candidate_headers,
        )
        payload = response.json()
        return int(payload.get("marked_now", 0)), int(payload.get("already_marked", 0))

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        attempts = self._settings.network_max_attempts
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            await asyncio.sleep(await self._limiter.reserve())

            try:
                response = await self._http.request(method, path, json=json, headers=headers)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == attempts:
                    break
                await asyncio.sleep(self._retry_delay(attempt))
                continue

            if response.status_code < 400:
                return response

            if response.status_code in (429, 403):
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                if response.status_code == 429:
                    # The real limit is unknown; slow down for the rest of the run.
                    await self._limiter.penalize()
                raise RetryAfterError(response.status_code, retry_after, path)

            if response.status_code == 404:
                raise NotFoundError(f"404 от {path}: {response.text[:200]}")

            if response.status_code == 422:
                raise UnprocessableError(f"422 от {path}: {response.text[:200]}")

            if response.status_code >= 500:
                last_error = ExternalServerError(
                    f"{response.status_code} от {path}: {response.text[:200]}"
                )
                if attempt == attempts:
                    break
                await asyncio.sleep(self._retry_delay(attempt))
                continue

            raise ExternalServerError(f"{response.status_code} от {path}")

        raise NetworkExhausted(
            f"{attempts} попыток к {path} закончились неудачей: {last_error}"
        ) from last_error

    def _retry_delay(self, attempt: int) -> float:
        return backoff_delay(attempt, self._settings.network_backoff_base_s, random.random())
