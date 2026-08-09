"""Digit statistics over a selection of files.

`total` is computed over the whole selection while the per-file table is
paginated — that is the reason the counters are stored as d0..d9 columns rather
than as a JSON blob: the totals are one `sum()` over the same WHERE clause, with
no need to read a single file body.
"""

import math

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DIGIT_COLUMNS, File
from app.schemas import DIGITS, PerFileItem, PerFilePage, StatsRequest, StatsResponse
from app.time import to_display, utc_now


def selection_condition(request: StatsRequest, as_of) -> ColumnElement[bool]:
    """WHERE clause shared by the totals and the paginated breakdown.

    Both must use exactly the same condition, otherwise the totals stop matching
    the rows they are supposed to summarise.
    """
    condition = File.downloaded_at <= as_of
    if not request.select_all:
        condition = condition & File.id.in_(request.file_ids)
    return condition


async def compute_stats(session: AsyncSession, request: StatsRequest) -> StatsResponse:
    """Return totals over the selection plus one page of the per-file breakdown."""
    # Substituted here and echoed in the response: the client has to send the same
    # value back when paging, or a download running in parallel shifts the
    # selection out from under it.
    as_of = request.as_of or utc_now()
    condition = selection_condition(request, as_of)

    selected_count, *sums = (
        await session.execute(
            select(
                func.count(File.id),
                *(func.coalesce(func.sum(column), 0) for column in DIGIT_COLUMNS),
            ).where(condition)
        )
    ).one()

    rows = (
        await session.execute(
            select(File.id, File.name, File.downloaded_at, *DIGIT_COLUMNS)
            .where(condition)
            # The same ordering key as /api/files: a different one would let the
            # pages of the two tables disagree about what page 2 contains.
            .order_by(File.downloaded_at, File.id)
            .limit(request.size)
            .offset((request.page - 1) * request.size)
        )
    ).all()

    return StatsResponse(
        as_of=as_of,
        selected_count=selected_count,
        total=dict(zip(DIGITS, (int(value) for value in sums), strict=True)),
        per_file=PerFilePage(
            page=request.page,
            size=request.size,
            total_pages=math.ceil(selected_count / request.size),
            items=[
                PerFileItem(
                    id=row[0],
                    name=row[1],
                    downloaded_at_nsk=to_display(row[2]),
                    counts=dict(zip(DIGITS, row[3:], strict=True)),
                )
                for row in rows
            ],
        ),
    )
