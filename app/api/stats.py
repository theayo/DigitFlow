"""Digit statistics over a selection of files."""

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import StatsRequest, StatsResponse
from app.services.stats import compute_stats

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.post("", response_model=StatsResponse)
async def stats(
    request: StatsRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> StatsResponse:
    """Totals over the whole selection plus one page of the per-file breakdown.

    A POST rather than a GET: the selection can carry up to `MAX_FILE_IDS`
    identifiers, which does not belong in a query string.
    """
    return await compute_stats(session, request)
