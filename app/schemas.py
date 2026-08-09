"""Request and response schemas of the internal API.

Every timestamp handed to the UI is converted to the display timezone at this
boundary and named with an `_nsk` suffix, so a value that skipped the conversion
is visible in the response itself. The two exceptions are `retry_at` and `as_of`:
they are UTC on purpose — the countdown is computed by the browser, and `as_of`
travels back to the server on the next page request.
"""

from datetime import datetime
from typing import Self

from pydantic import BaseModel, Field, field_validator, model_validator

from app.config import get_settings
from app.time import to_utc

# Digit keys of the statistics tables, as strings: JSON object keys are strings
# anyway, and spelling that out here keeps the response shape explicit.
DIGITS: tuple[str, ...] = tuple(str(digit) for digit in range(10))


# --- runs -------------------------------------------------------------------


class EventOut(BaseModel):
    """One entry of the run log."""

    id: int
    ts_nsk: datetime
    level: str
    message: str


class RunOut(BaseModel):
    """State of a run as the download page needs it.

    `retry_at` and `retry_reason` are filled only while the run is waiting, and
    `error` only when it has failed.
    """

    run_id: int
    status: str
    active: bool
    started_at_nsk: datetime
    finished_at_nsk: datetime | None = None
    names_seen: int
    files_saved: int
    retry_at: datetime | None = None
    retry_reason: str | None = None
    error: str | None = None
    events: list[EventOut] = Field(default_factory=list)


# --- files ------------------------------------------------------------------


class FileOut(BaseModel):
    id: int
    name: str
    downloaded_at_nsk: datetime


class FilesPage(BaseModel):
    page: int
    size: int
    total: int
    total_pages: int
    items: list[FileOut]


# --- statistics -------------------------------------------------------------


class StatsRequest(BaseModel):
    """Selection of files to compute digit statistics over.

    The limits live in the validator rather than in `Field` constraints because
    they are configurable (§ 11) and a schema constant would freeze them.
    """

    select_all: bool = False
    file_ids: list[int] = Field(default_factory=list)
    as_of: datetime | None = None
    page: int = Field(default=1, ge=1)
    size: int = Field(default=50, ge=1)

    @field_validator("as_of")
    @classmethod
    def _reject_naive(cls, value: datetime | None) -> datetime | None:
        # A naive value carries no offset, so honouring it would mean guessing one.
        return to_utc(value) if value is not None else None

    @model_validator(mode="after")
    def _check_selection(self) -> Self:
        settings = get_settings()

        if self.select_all == bool(self.file_ids):
            raise ValueError(
                "нужно передать ровно одно: select_all=true или непустой список file_ids"
            )
        if len(self.file_ids) > settings.max_file_ids:
            raise ValueError(
                f"передано {len(self.file_ids)} идентификаторов, предел — {settings.max_file_ids}"
            )
        if self.size > settings.stats_page_size_max:
            raise ValueError(
                f"размер страницы {self.size} превышает предел {settings.stats_page_size_max}"
            )
        return self


class PerFileItem(BaseModel):
    id: int
    name: str
    downloaded_at_nsk: datetime
    counts: dict[str, int]


class PerFilePage(BaseModel):
    page: int
    size: int
    total_pages: int
    items: list[PerFileItem]


class StatsResponse(BaseModel):
    """Totals over the whole selection plus one page of the per-file breakdown.

    `as_of` is echoed back — including the value the server substituted — because
    the client has to send it again when it asks for the next page, otherwise a
    download running in parallel shifts the selection between pages.
    """

    as_of: datetime
    selected_count: int
    total: dict[str, int]
    per_file: PerFilePage
