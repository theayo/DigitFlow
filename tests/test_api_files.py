"""GET /api/files: pagination and sorting by download time."""

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from tests.factories import build_file

BASE = datetime(2026, 8, 8, 6, 0, tzinfo=UTC)


async def seed(sessionmaker: async_sessionmaker[AsyncSession], count: int) -> None:
    async with sessionmaker() as session:
        for index in range(count):
            session.add(build_file(f"file-{index:03d}.txt", BASE + timedelta(minutes=index)))
        await session.commit()


async def test_empty_listing(api, sessionmaker, clean_db) -> None:
    body = (await api.get("/api/files")).json()

    assert body == {"page": 1, "size": 50, "total": 0, "total_pages": 0, "items": []}


async def test_newest_first_by_default(api, sessionmaker, clean_db) -> None:
    await seed(sessionmaker, 5)

    body = (await api.get("/api/files")).json()

    assert [item["name"] for item in body["items"]] == [
        "file-004.txt",
        "file-003.txt",
        "file-002.txt",
        "file-001.txt",
        "file-000.txt",
    ]
    assert body["total"] == 5
    assert body["total_pages"] == 1


async def test_ascending_order(api, sessionmaker, clean_db) -> None:
    await seed(sessionmaker, 3)

    body = (await api.get("/api/files", params={"order": "asc"})).json()

    assert [item["name"] for item in body["items"]] == [
        "file-000.txt",
        "file-001.txt",
        "file-002.txt",
    ]


async def test_pagination_slices_without_gaps(api, sessionmaker, clean_db) -> None:
    await seed(sessionmaker, 5)

    pages = [
        (await api.get("/api/files", params={"page": page, "size": 2, "order": "asc"})).json()
        for page in (1, 2, 3)
    ]

    assert [item["name"] for page in pages for item in page["items"]] == [
        f"file-{index:03d}.txt" for index in range(5)
    ]
    assert pages[0]["total_pages"] == 3
    assert pages[2]["items"] == [
        {
            "id": pages[2]["items"][0]["id"],
            "name": "file-004.txt",
            "downloaded_at_nsk": pages[2]["items"][0]["downloaded_at_nsk"],
        }
    ]


async def test_download_time_is_rendered_in_the_display_timezone(
    api, sessionmaker, clean_db
) -> None:
    """Stored in UTC, shown in Novosibirsk time — 06:00Z is 13:00 there."""
    await seed(sessionmaker, 1)

    body = (await api.get("/api/files")).json()

    assert body["items"][0]["downloaded_at_nsk"].startswith("2026-08-08T13:00:00")
    assert body["items"][0]["downloaded_at_nsk"].endswith("+07:00")


async def test_page_size_over_the_limit_is_rejected(api, clean_db) -> None:
    limit = get_settings().files_page_size_max

    response = await api.get("/api/files", params={"size": limit + 1})

    assert response.status_code == 422


async def test_page_below_one_is_rejected(api, clean_db) -> None:
    assert (await api.get("/api/files", params={"page": 0})).status_code == 422
