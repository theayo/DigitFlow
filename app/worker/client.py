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
from typing import Protocol

import httpx

from app.config import Settings
from app.time import utc_now
from app.worker.errors import (
    ExternalServerError,
    InvalidResponseError,
    NetworkExhausted,
    NotFoundError,
    RetryAfterError,
    UnprocessableError,
)

NAMES_PATH = "/api/files/names"
DOWNLOAD_PATH = "/api/files/download"
DOWNLOADED_PATH = "/api/files/downloaded"

# The external API accepts at most three names per download request.
MAX_NAMES_PER_DOWNLOAD = 3


class RateLimiter(Protocol):
    """Pace contract required by the external API client."""

    async def reserve(self) -> float: ...

    async def penalize(self) -> int: ...

    async def reset(self) -> None: ...


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


def _json_object(response: httpx.Response, endpoint: str) -> dict:
    """Decode a successful body, insisting that it is a JSON object.

    A 2xx status says nothing about the shape of what came back, and every value
    below is read positionally. `payload.get(...)` on something that is not a
    mapping would either explode later or, worse, quietly produce a default.
    """
    try:
        payload = response.json()
    except ValueError as exc:
        raise InvalidResponseError(f"{endpoint}: тело ответа не является JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise InvalidResponseError(
            f"{endpoint}: ожидался JSON-объект, получен {type(payload).__name__}"
        )
    return payload


def parse_names(payload: dict, endpoint: str) -> list[str]:
    """Validate the body of /names and return the names.

    Strict on purpose. An absent `file_names` used to become an empty list, and
    an empty list is the one and only signal that the catalog is finished
    (§ 2) — so a malformed answer would end the run as `done` with the catalog
    half downloaded. That failure is silent and unrecoverable: the identifier is
    single-use.
    """
    if "file_names" not in payload:
        raise InvalidResponseError(f"{endpoint}: в ответе нет поля file_names")

    names = payload["file_names"]
    if not isinstance(names, list):
        # A string would otherwise iterate into a list of single characters.
        raise InvalidResponseError(
            f"{endpoint}: file_names должен быть списком, получен {type(names).__name__}"
        )

    for name in names:
        if not isinstance(name, str):
            raise InvalidResponseError(
                f"{endpoint}: имя файла должно быть строкой, получено {name!r}"
            )
        if not name.strip():
            raise InvalidResponseError(f"{endpoint}: пустое имя файла в ответе")

    if len(set(names)) != len(names):
        # Requesting the same name twice in one chunk would fail archive
        # validation as a duplicate entry (§ 8.2.3), and the run would then hunt
        # for a guilty file that does not exist.
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise InvalidResponseError(f"{endpoint}: в ответе повторяются имена {duplicates}")

    return list(names)


def parse_marked(payload: dict, sent: int, endpoint: str) -> tuple[int, int]:
    """Validate the body of /downloaded and return (marked_now, already_marked).

    These two numbers are not decoration: `marked_now` feeds the stale-iteration
    guard (§ 8.4), which is what stops a run that confirms nothing from looping
    forever. A missing counter silently read as zero would either fake progress
    or fake its absence.
    """
    counters: list[int] = []
    for field in ("marked_now", "already_marked"):
        if field not in payload:
            raise InvalidResponseError(f"{endpoint}: в ответе нет поля {field}")

        value = payload[field]
        # bool is a subclass of int, and `True` as a counter is nonsense.
        if isinstance(value, bool) or not isinstance(value, int):
            raise InvalidResponseError(
                f"{endpoint}: {field} должен быть целым числом, получено {value!r}"
            )
        if value < 0:
            raise InvalidResponseError(f"{endpoint}: {field} отрицательный: {value}")
        counters.append(value)

    marked_now, already_marked = counters
    if marked_now + already_marked != sent:
        raise InvalidResponseError(
            f"{endpoint}: счётчики не сходятся с запросом: отправлено {sent} имён, "
            f"в ответе marked_now={marked_now} и already_marked={already_marked}"
        )
    return marked_now, already_marked


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
        return parse_names(_json_object(response, NAMES_PATH), NAMES_PATH)

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
        return parse_marked(_json_object(response, DOWNLOADED_PATH), len(names), DOWNLOADED_PATH)

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
