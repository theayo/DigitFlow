"""Listing of downloaded files."""

import math
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.models import File
from app.schemas import FileOut, FilesPage
from app.time import to_display

router = APIRouter(prefix="/api/files", tags=["files"])


@router.get("", response_model=FilesPage)
async def list_files(
    session: Annotated[AsyncSession, Depends(get_session)],
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1)] = 50,
    order: Literal["asc", "desc"] = "desc",
) -> FilesPage:
    """One page of files, sorted by download time."""
    settings = get_settings()
    if size > settings.files_page_size_max:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"размер страницы {size} превышает предел {settings.files_page_size_max}",
        )

    total = (await session.execute(select(func.count(File.id)))).scalar_one()

    # Ordering by downloaded_at alone is not deterministic: files saved within the
    # same chunk share a timestamp closely enough to swap places between pages.
    ordering = (
        (File.downloaded_at.asc(), File.id.asc())
        if order == "asc"
        else (File.downloaded_at.desc(), File.id.desc())
    )
    rows = (
        await session.execute(
            select(File.id, File.name, File.downloaded_at)
            .order_by(*ordering)
            .limit(size)
            .offset((page - 1) * size)
        )
    ).all()

    return FilesPage(
        page=page,
        size=size,
        total=total,
        total_pages=math.ceil(total / size),
        items=[
            FileOut(id=row.id, name=row.name, downloaded_at_nsk=to_display(row.downloaded_at))
            for row in rows
        ],
    )
