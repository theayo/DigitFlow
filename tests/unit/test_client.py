"""Unit tests for pacing helpers and the external API client."""

from datetime import timedelta

import httpx
import pytest
import respx

from app.config import get_settings
from app.time import utc_now
from app.worker.client import (
    DOWNLOAD_PATH,
    DOWNLOADED_PATH,
    NAMES_PATH,
    FilesApiClient,
    backoff_delay,
    parse_retry_after,
)
from app.worker.downloader import chunked
from app.worker.errors import (
    NetworkExhausted,
    NotFoundError,
    RetryAfterError,
    UnprocessableError,
)

BASE_URL = "http://external.test"


class StubLimiter:
    """Records penalties and never actually sleeps."""

    def __init__(self) -> None:
        self.penalties = 0

    async def reserve(self) -> float:
        return 0.0

    async def penalize(self) -> int:
        self.penalties += 1
        return 0


def build_client(limiter: StubLimiter, http: httpx.AsyncClient) -> FilesApiClient:
    settings = get_settings().model_copy(
        update={"network_max_attempts": 3, "network_backoff_base_s": 0.0}
    )
    return FilesApiClient(settings, limiter, http)


# --- chunking ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("count", "expected"),
    [(3, [3]), (9, [3, 3, 3]), (7, [3, 3, 1]), (1, [1])],
)
def test_names_are_chunked_by_three(count: int, expected: list[int]) -> None:
    names = [f"{i}.txt" for i in range(count)]

    assert [len(chunk) for chunk in chunked(names, 3)] == expected


# --- Retry-After ------------------------------------------------------------


def test_retry_after_in_seconds() -> None:
    assert parse_retry_after("30") == 30.0


def test_retry_after_as_http_date() -> None:
    deadline = utc_now() + timedelta(seconds=120)
    header = deadline.strftime("%a, %d %b %Y %H:%M:%S GMT")

    value = parse_retry_after(header)

    assert value is not None
    assert 100 <= value <= 125


def test_retry_after_absent() -> None:
    assert parse_retry_after(None) is None


def test_retry_after_garbage() -> None:
    assert parse_retry_after("soon-ish") is None


# --- backoff ----------------------------------------------------------------


def test_backoff_grows_and_stays_within_jitter_band() -> None:
    lowest = [backoff_delay(attempt, 1.0, 0.0) for attempt in (1, 2, 3)]
    highest = [backoff_delay(attempt, 1.0, 0.999) for attempt in (1, 2, 3)]

    assert lowest == [0.5, 1.0, 2.0]
    assert lowest < highest
    for attempt, low, high in zip((1, 2, 3), lowest, highest, strict=True):
        ceiling = 2 ** (attempt - 1)
        assert low == pytest.approx(ceiling / 2)
        assert high <= ceiling


# --- client behaviour -------------------------------------------------------


async def test_candidate_header_only_where_accepted() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        names_route = mock.get(NAMES_PATH).respond(json={"file_names": ["a.txt"]})
        download_route = mock.post(DOWNLOAD_PATH).respond(content=b"zip")
        marked_route = mock.post(DOWNLOADED_PATH).respond(
            json={"marked_now": 1, "already_marked": 0}
        )

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            client = build_client(limiter, http)
            await client.get_names()
            await client.download(["a.txt"])
            await client.mark_downloaded(["a.txt"])

    assert "x-candidate-id" in names_route.calls[0].request.headers
    assert "x-candidate-id" in marked_route.calls[0].request.headers
    # /download has no such parameter in the specification.
    assert "x-candidate-id" not in download_route.calls[0].request.headers


async def test_429_raises_and_slows_the_pace() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(status_code=429, headers={"Retry-After": "12"})

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(RetryAfterError) as caught:
                await build_client(limiter, http).get_names()

    assert caught.value.status_code == 429
    assert caught.value.retry_after == 12.0
    assert limiter.penalties == 1


async def test_403_does_not_change_the_pace() -> None:
    """A ban is not a hint about the rate limit, so the interval stays put."""
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(status_code=403, headers={"Retry-After": "1800"})

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(RetryAfterError) as caught:
                await build_client(limiter, http).get_names()

    assert caught.value.retry_after == 1800.0
    assert limiter.penalties == 0


async def test_404_is_surfaced() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.post(DOWNLOAD_PATH).respond(status_code=404, json={"detail": "нет файла"})

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(NotFoundError):
                await build_client(limiter, http).download(["a.txt"])


async def test_422_is_not_retried() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        route = mock.get(NAMES_PATH).respond(status_code=422, json={"detail": []})

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(UnprocessableError):
                await build_client(limiter, http).get_names()

    assert route.call_count == 1


async def test_5xx_is_retried_then_exhausted() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        route = mock.get(NAMES_PATH).respond(status_code=503)

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(NetworkExhausted):
                await build_client(limiter, http).get_names()

    assert route.call_count == 3


async def test_transient_5xx_recovers() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(200, json={"file_names": ["a.txt"]}),
            ]
        )

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            names = await build_client(limiter, http).get_names()

    assert names == ["a.txt"]


async def test_network_error_is_retried_then_exhausted() -> None:
    limiter = StubLimiter()
    async with respx.mock(base_url=BASE_URL) as mock:
        route = mock.get(NAMES_PATH).mock(side_effect=httpx.ConnectError("нет связи"))

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(NetworkExhausted):
                await build_client(limiter, http).get_names()

    assert route.call_count == 3


async def test_download_refuses_more_than_three_names() -> None:
    limiter = StubLimiter()
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        with pytest.raises(ValueError, match="1..3"):
            await build_client(limiter, http).download(["a", "b", "c", "d"])
