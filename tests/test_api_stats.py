"""POST /api/stats: totals over a selection, breakdown per file."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import CONTENT_LENGTH
from app.time import utc_now
from tests.factories import build_file

# Kept relative to the current moment: `as_of` defaults to "now", and files dated
# in the future would silently drop out of every selection.
BASE = utc_now() - timedelta(hours=1)

ZERO_TOTAL = {str(digit): 0 for digit in range(10)}


async def seed_digits(
    sessionmaker: async_sessionmaker[AsyncSession], digits: list[int]
) -> list[int]:
    """One file per digit, filled with that digit 500 times. Returns their ids."""
    async with sessionmaker() as session:
        files = [
            build_file(
                f"digit-{digit}-{index}.txt",
                BASE + timedelta(minutes=index),
                content=str(digit) * CONTENT_LENGTH,
            )
            for index, digit in enumerate(digits)
        ]
        session.add_all(files)
        await session.commit()
        return [file.id for file in files]


async def test_select_all_totals_every_file(api, sessionmaker, clean_db) -> None:
    await seed_digits(sessionmaker, [0, 1, 2])

    body = (await api.post("/api/stats", json={"select_all": True})).json()

    assert body["selected_count"] == 3
    assert body["total"] == ZERO_TOTAL | {"0": 500, "1": 500, "2": 500}
    assert body["per_file"]["total_pages"] == 1
    assert [item["counts"]["0"] for item in body["per_file"]["items"]] == [500, 0, 0]


async def test_totals_cover_the_selection_not_the_page(api, sessionmaker, clean_db) -> None:
    """The whole point of storing counters in columns: totals ignore pagination."""
    await seed_digits(sessionmaker, [1, 1, 1, 1, 1])

    pages = [
        (await api.post("/api/stats", json={"select_all": True, "page": page, "size": 2})).json()
        for page in (1, 2, 3)
    ]

    for body in pages:
        assert body["total"] == ZERO_TOTAL | {"1": 2500}
        assert body["selected_count"] == 5
        assert body["per_file"]["total_pages"] == 3
    assert [len(body["per_file"]["items"]) for body in pages] == [2, 2, 1]

    names = [item["name"] for body in pages for item in body["per_file"]["items"]]
    assert names == sorted(names), "страницы обязаны идти в порядке скачивания, без перемешивания"


async def test_file_ids_select_a_subset(api, sessionmaker, clean_db) -> None:
    ids = await seed_digits(sessionmaker, [3, 4, 5])

    body = (await api.post("/api/stats", json={"file_ids": [ids[0], ids[2]]})).json()

    assert body["selected_count"] == 2
    assert body["total"] == ZERO_TOTAL | {"3": 500, "5": 500}
    assert [item["id"] for item in body["per_file"]["items"]] == [ids[0], ids[2]]


async def test_as_of_freezes_the_selection(api, sessionmaker, clean_db) -> None:
    """A download running in parallel must not shift the selection between pages."""
    await seed_digits(sessionmaker, [7, 7])

    first = (await api.post("/api/stats", json={"select_all": True, "size": 1})).json()
    as_of = first["as_of"]

    # A file arrives after the cut-off, exactly as a parallel run would produce.
    async with sessionmaker() as session:
        session.add(build_file("late.txt", utc_now(), content="8" * CONTENT_LENGTH))
        await session.commit()

    second = (
        await api.post(
            "/api/stats", json={"select_all": True, "size": 1, "page": 2, "as_of": as_of}
        )
    ).json()

    assert second["as_of"] == as_of
    assert second["selected_count"] == first["selected_count"] == 2
    assert second["total"] == first["total"] == ZERO_TOTAL | {"7": 1000}

    # Without the cut-off the newcomer is included — the selection really did move.
    fresh = (await api.post("/api/stats", json={"select_all": True, "size": 1})).json()
    assert fresh["selected_count"] == 3


async def test_as_of_is_returned_when_the_server_substitutes_it(
    api, sessionmaker, clean_db
) -> None:
    await seed_digits(sessionmaker, [0])

    body = (await api.post("/api/stats", json={"select_all": True})).json()

    assert body["as_of"].endswith("Z"), "отсечка отдаётся в UTC"


async def test_empty_selection_yields_zero_totals(api, sessionmaker, clean_db) -> None:
    body = (await api.post("/api/stats", json={"file_ids": [12345]})).json()

    assert body["selected_count"] == 0
    assert body["total"] == ZERO_TOTAL
    assert body["per_file"]["items"] == []
    assert body["per_file"]["total_pages"] == 0


async def test_download_time_is_rendered_in_the_display_timezone(
    api, sessionmaker, clean_db
) -> None:
    await seed_digits(sessionmaker, [2])

    body = (await api.post("/api/stats", json={"select_all": True})).json()

    assert body["per_file"]["items"][0]["downloaded_at_nsk"].endswith("+07:00")


# --- rejected requests ------------------------------------------------------


async def test_both_selection_modes_are_rejected(api, clean_db) -> None:
    response = await api.post("/api/stats", json={"select_all": True, "file_ids": [1]})

    assert response.status_code == 422


async def test_no_selection_mode_is_rejected(api, clean_db) -> None:
    assert (await api.post("/api/stats", json={})).status_code == 422
    assert (await api.post("/api/stats", json={"file_ids": []})).status_code == 422
    assert (await api.post("/api/stats", json={"select_all": False})).status_code == 422


async def test_too_many_file_ids_are_rejected(api, clean_db) -> None:
    limit = get_settings().max_file_ids

    response = await api.post("/api/stats", json={"file_ids": list(range(limit + 1))})

    assert response.status_code == 422


async def test_page_size_over_the_limit_is_rejected(api, clean_db) -> None:
    limit = get_settings().stats_page_size_max

    response = await api.post("/api/stats", json={"select_all": True, "size": limit + 1})

    assert response.status_code == 422


async def test_naive_as_of_is_rejected(api, clean_db) -> None:
    """A naive timestamp carries no offset; honouring it would mean guessing one."""
    response = await api.post(
        "/api/stats", json={"select_all": True, "as_of": "2026-08-08T13:00:00"}
    )

    assert response.status_code == 422
