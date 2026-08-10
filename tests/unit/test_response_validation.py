"""Validation of what the external API sends back on a 2xx (§ 2).

A successful status says nothing about the body, and two of these fields carry
decisions the run cannot take back: an empty `file_names` is the one signal that
the catalog is finished, and `marked_now` is what the stale-iteration guard reads
as progress. A missing field quietly read as a default would end a half
downloaded catalog as `done` — with a single-use identifier, that is
unrecoverable.
"""

import json

import httpx
import pytest
import respx

from app.config import get_settings
from app.worker.client import (
    DOWNLOADED_PATH,
    NAMES_PATH,
    FilesApiClient,
    parse_marked,
    parse_names,
)
from app.worker.errors import InvalidResponseError

BASE_URL = "http://external.test"


class StubLimiter:
    async def reserve(self) -> float:
        return 0.0

    async def penalize(self) -> int:
        return 0

    async def reset(self) -> None:
        return None


def build_client(http: httpx.AsyncClient) -> FilesApiClient:
    settings = get_settings().model_copy(
        update={"network_max_attempts": 2, "network_backoff_base_s": 0.0}
    )
    return FilesApiClient(settings, StubLimiter(), http)


# --- /names -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ({"file_names": []}, []),
        ({"file_names": ["a.txt"]}, ["a.txt"]),
        ({"file_names": ["a.txt", "b.txt"]}, ["a.txt", "b.txt"]),
        # Unknown extra fields are none of our business.
        ({"file_names": ["a.txt"], "total": 17}, ["a.txt"]),
    ],
)
def test_valid_names_are_accepted(body: dict, expected: list[str]) -> None:
    assert parse_names(body, NAMES_PATH) == expected


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ({}, "нет поля file_names"),
        ({"files": ["a.txt"]}, "нет поля file_names"),
        ({"file_names": None}, "должен быть списком"),
        # Would otherwise iterate into ['a', '.', 't', 'x', 't'].
        ({"file_names": "a.txt"}, "должен быть списком"),
        ({"file_names": {"a.txt": 1}}, "должен быть списком"),
        ({"file_names": ["a.txt", 42]}, "должно быть строкой"),
        ({"file_names": ["a.txt", None]}, "должно быть строкой"),
        ({"file_names": [""]}, "пустое имя"),
        ({"file_names": ["   "]}, "пустое имя"),
        ({"file_names": ["a.txt", "a.txt"]}, "повторяются имена"),
    ],
)
def test_broken_names_are_rejected(body: dict, fragment: str) -> None:
    with pytest.raises(InvalidResponseError) as error:
        parse_names(body, NAMES_PATH)

    assert fragment in str(error.value)


async def test_names_body_that_is_not_json_is_rejected() -> None:
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(200, content=b"<html>oops</html>")

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(InvalidResponseError) as error:
                await build_client(http).get_names()

    assert "не является JSON" in str(error.value)


async def test_names_body_that_is_a_json_array_is_rejected() -> None:
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.get(NAMES_PATH).respond(200, json=["a.txt"])

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(InvalidResponseError) as error:
                await build_client(http).get_names()

    assert "ожидался JSON-объект" in str(error.value)


async def test_a_broken_names_body_is_not_retried() -> None:
    """The request succeeded; asking again would return the same body."""
    async with respx.mock(base_url=BASE_URL) as mock:
        route = mock.get(NAMES_PATH).respond(200, json={"total": 0})

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(InvalidResponseError):
                await build_client(http).get_names()

    assert route.call_count == 1


# --- /downloaded ------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "sent", "expected"),
    [
        ({"marked_now": 2, "already_marked": 1}, 3, (2, 1)),
        ({"marked_now": 0, "already_marked": 3}, 3, (0, 3)),
        ({"marked_now": 1, "already_marked": 0, "extra": "x"}, 1, (1, 0)),
    ],
)
def test_valid_counters_are_accepted(body: dict, sent: int, expected: tuple[int, int]) -> None:
    assert parse_marked(body, sent, DOWNLOADED_PATH) == expected


@pytest.mark.parametrize(
    ("body", "sent", "fragment"),
    [
        ({"already_marked": 1}, 1, "нет поля marked_now"),
        ({"marked_now": 1}, 1, "нет поля already_marked"),
        ({"marked_now": "1", "already_marked": 0}, 1, "должен быть целым числом"),
        ({"marked_now": 1.0, "already_marked": 0}, 1, "должен быть целым числом"),
        # bool is a subclass of int, and True as a count is nonsense.
        ({"marked_now": True, "already_marked": 0}, 1, "должен быть целым числом"),
        ({"marked_now": None, "already_marked": 0}, 1, "должен быть целым числом"),
        ({"marked_now": -1, "already_marked": 2}, 1, "отрицательный"),
        ({"marked_now": 1, "already_marked": 0}, 3, "не сходятся с запросом"),
        ({"marked_now": 3, "already_marked": 3}, 3, "не сходятся с запросом"),
    ],
)
def test_broken_counters_are_rejected(body: dict, sent: int, fragment: str) -> None:
    with pytest.raises(InvalidResponseError) as error:
        parse_marked(body, sent, DOWNLOADED_PATH)

    assert fragment in str(error.value)


async def test_downloaded_body_that_is_not_json_is_rejected() -> None:
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.post(DOWNLOADED_PATH).respond(200, content=b"OK")

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(InvalidResponseError) as error:
                await build_client(http).mark_downloaded(["a.txt"])

    assert "не является JSON" in str(error.value)


async def test_counters_are_checked_against_what_was_sent() -> None:
    """The client knows how many names it sent; the answer has to account for them."""
    async with respx.mock(base_url=BASE_URL) as mock:
        mock.post(DOWNLOADED_PATH).mock(
            side_effect=lambda request: httpx.Response(
                200,
                json={
                    "marked_now": len(json.loads(request.content)["file_names"]) - 1,
                    "already_marked": 0,
                },
            )
        )

        async with httpx.AsyncClient(base_url=BASE_URL) as http:
            with pytest.raises(InvalidResponseError) as error:
                await build_client(http).mark_downloaded(["a.txt", "b.txt"])

    assert "отправлено 2" in str(error.value)
