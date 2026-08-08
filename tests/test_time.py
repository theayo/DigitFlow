"""Unit tests for the time helpers."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.time import ensure_aware, to_display, to_utc, utc_now


def test_utc_now_is_aware_utc() -> None:
    """utc_now() must return an aware datetime pinned to UTC."""
    now = utc_now()

    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)
    assert now.tzinfo is UTC


def test_utc_converted_to_novosibirsk() -> None:
    """Asia/Novosibirsk is UTC+7 with no DST, so the wall clock shifts by 7 hours."""
    moment = datetime(2026, 8, 8, 13, 45, 0, tzinfo=UTC)

    displayed = to_display(moment)

    assert displayed.utcoffset() == timedelta(hours=7)
    assert displayed.replace(tzinfo=None) == datetime(2026, 8, 8, 20, 45, 0)
    # Converting only relabels the same instant.
    assert displayed == moment


def test_display_conversion_keeps_instant_for_other_offsets() -> None:
    """A value in a third timezone must land on the same instant, not on its wall clock."""
    moment = datetime(2026, 8, 8, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    displayed = to_display(moment)

    assert displayed.replace(tzinfo=None) == datetime(2026, 8, 8, 14, 0, 0)
    assert displayed == moment


@pytest.mark.parametrize("convert", [ensure_aware, to_display, to_utc])
def test_naive_datetime_rejected(convert: Callable[[datetime], datetime]) -> None:
    """Naive input carries no offset, so guessing one is an error, not a default."""
    naive = datetime(2026, 8, 8, 13, 45, 0)

    with pytest.raises(ValueError, match="naive datetime"):
        convert(naive)
