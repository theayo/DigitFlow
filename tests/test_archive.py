"""Unit tests for archive validation. One test per rule of the specification."""

import io
import zipfile

import pytest

from app.worker.archive import CONTENT_LENGTH, content_hash, digit_counts, parse_archive
from app.worker.errors import ArchiveError

MAX_ENTRY = 65536


def build_zip(entries: list[tuple[str, str | bytes]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in entries:
            archive.writestr(name, payload)
    return buffer.getvalue()


def content(*, digit: str = "7", length: int = CONTENT_LENGTH) -> str:
    return digit * length


def test_valid_archive_is_parsed() -> None:
    payload = build_zip([("a.txt", content()), ("b.txt", "0123456789" * 50)])

    parsed = parse_archive(payload, ["a.txt", "b.txt"], MAX_ENTRY)

    assert {file.name for file in parsed} == {"a.txt", "b.txt"}
    first = next(file for file in parsed if file.name == "a.txt")
    assert first.digit_counts[7] == CONTENT_LENGTH
    assert sum(first.digit_counts) == CONTENT_LENGTH
    assert first.content_hash == content_hash(content())


def test_trailing_newline_is_tolerated() -> None:
    payload = build_zip([("a.txt", content() + "\r\n")])

    parsed = parse_archive(payload, ["a.txt"], MAX_ENTRY)

    assert len(parsed[0].content) == CONTENT_LENGTH


def test_not_a_zip_is_rejected() -> None:
    with pytest.raises(ArchiveError, match="ZIP"):
        parse_archive(b"definitely not a zip", ["a.txt"], MAX_ENTRY)


def test_extra_entry_is_rejected() -> None:
    payload = build_zip([("a.txt", content()), ("surprise.txt", content())])

    with pytest.raises(ArchiveError, match="не совпадает"):
        parse_archive(payload, ["a.txt"], MAX_ENTRY)


def test_missing_entry_is_rejected() -> None:
    payload = build_zip([("a.txt", content())])

    with pytest.raises(ArchiveError, match="не совпадает"):
        parse_archive(payload, ["a.txt", "b.txt"], MAX_ENTRY)


def test_duplicate_entry_is_rejected() -> None:
    payload = build_zip([("a.txt", content()), ("a.txt", content(digit="1"))])

    with pytest.raises(ArchiveError, match="повторяющиеся"):
        parse_archive(payload, ["a.txt"], MAX_ENTRY)


@pytest.mark.parametrize("name", ["../a.txt", "nested/a.txt", "dir\\a.txt"])
def test_path_in_entry_name_is_rejected(name: str) -> None:
    payload = build_zip([(name, content())])

    with pytest.raises(ArchiveError):
        parse_archive(payload, [name], MAX_ENTRY)


def test_oversized_entry_is_rejected() -> None:
    payload = build_zip([("a.txt", "1" * 5000)])

    with pytest.raises(ArchiveError, match="слишком велика"):
        parse_archive(payload, ["a.txt"], max_entry_bytes=1000)


def test_non_ascii_entry_is_rejected() -> None:
    payload = build_zip([("a.txt", "я" * CONTENT_LENGTH)])

    with pytest.raises(ArchiveError, match="ASCII"):
        parse_archive(payload, ["a.txt"], MAX_ENTRY)


@pytest.mark.parametrize("length", [CONTENT_LENGTH - 1, CONTENT_LENGTH + 1])
def test_wrong_length_is_rejected(length: int) -> None:
    payload = build_zip([("a.txt", content(length=length))])

    with pytest.raises(ArchiveError, match="не является"):
        parse_archive(payload, ["a.txt"], MAX_ENTRY)


def test_letter_instead_of_digit_is_rejected() -> None:
    payload = build_zip([("a.txt", "7" * (CONTENT_LENGTH - 1) + "x")])

    with pytest.raises(ArchiveError, match="не является"):
        parse_archive(payload, ["a.txt"], MAX_ENTRY)


def test_digit_counts_sum_to_content_length() -> None:
    counts = digit_counts("0123456789" * 50)

    assert sum(counts) == CONTENT_LENGTH
    assert counts == (50,) * 10
