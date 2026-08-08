"""Time helpers.

The application works exclusively with timezone-aware UTC datetimes: they are
produced by `utc_now()`, stored in `timestamptz` columns and sorted as UTC.
Converting to the display timezone happens only at the presentation boundary,
right before a value is handed to the UI.
"""

from datetime import UTC, datetime
from functools import lru_cache
from zoneinfo import ZoneInfo

from app.config import get_settings


def utc_now() -> datetime:
    """Return the current moment as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


@lru_cache
def get_display_timezone() -> ZoneInfo:
    """Return the timezone used for presenting timestamps to the user."""
    return ZoneInfo(get_settings().display_timezone)


def ensure_aware(value: datetime) -> datetime:
    """Return `value` unchanged, rejecting naive datetimes.

    A naive datetime carries no offset, so any conversion would have to guess
    one. Guessing silently produces timestamps that are wrong by hours, which is
    why this is an error rather than an assumption.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"naive datetime is not allowed: {value!r}")
    return value


def to_utc(value: datetime) -> datetime:
    """Convert an aware datetime to UTC."""
    return ensure_aware(value).astimezone(UTC)


def to_display(value: datetime) -> datetime:
    """Convert an aware datetime to the display timezone.

    Presentation boundary only — never call this on a value that is about to be
    stored or compared.
    """
    return ensure_aware(value).astimezone(get_display_timezone())
