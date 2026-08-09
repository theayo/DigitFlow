"""Unpacking and validating the ZIP returned by /api/files/download.

The archive is untrusted input. Nothing from it is saved unless every rule below
holds for every entry, because a half-accepted archive would leave files that can
neither be confirmed nor skipped.
"""

import hashlib
import io
import re
import zipfile
import zlib
from collections.abc import Sequence
from dataclasses import dataclass

from app.worker.errors import ArchiveError

CONTENT_LENGTH = 500
CONTENT_PATTERN = re.compile(rf"^[0-9]{{{CONTENT_LENGTH}}}$")


@dataclass(frozen=True)
class ParsedFile:
    """A validated file together with everything the database needs."""

    name: str
    content: str
    content_hash: str
    digit_counts: tuple[int, ...]


def digit_counts(content: str) -> tuple[int, ...]:
    counts = [0] * 10
    for char in content:
        counts[ord(char) - 48] += 1
    return tuple(counts)


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("ascii")).hexdigest()


def _reject_path(name: str) -> None:
    if "/" in name or "\\" in name or name in {".", ".."} or ".." in name.split("/"):
        raise ArchiveError(f"недопустимое имя записи в архиве: {name!r}")


def _read_entry(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    """Extract one entry, turning every expected failure into an `ArchiveError`.

    Opening the archive is not the only thing that can fail: a wrong CRC, an
    encrypted entry and an unsupported compression method all surface here, and
    each arrives as a different exception type. Left alone they escape as
    unexpected errors and skip the per-name isolation that is supposed to find
    out which file is at fault (§ 8.4).

    The list is deliberately exhaustive rather than a bare `except Exception`:
    anything not named here is a bug on our side, not a bad archive.
    """
    try:
        return archive.read(info)
    except zipfile.BadZipFile as exc:
        # Includes a CRC mismatch, reported as "Bad CRC-32".
        raise ArchiveError(f"запись {info.filename!r} не читается: {exc}") from exc
    except NotImplementedError as exc:
        # Before RuntimeError, which it subclasses — otherwise an unsupported
        # compression method would be reported as an unreadable entry.
        raise ArchiveError(
            f"запись {info.filename!r} сжата неподдерживаемым методом: {exc}"
        ) from exc
    except RuntimeError as exc:
        # What zipfile raises for an encrypted entry without a password.
        raise ArchiveError(f"запись {info.filename!r} не читается: {exc}") from exc
    except (EOFError, zlib.error) as exc:
        # Truncated entry, or a corrupt deflate stream.
        raise ArchiveError(f"запись {info.filename!r} повреждена: {exc}") from exc


def parse_archive(
    payload: bytes,
    expected: Sequence[str],
    max_entry_bytes: int,
) -> list[ParsedFile]:
    """Validate the archive and return its files.

    Raises `ArchiveError` on any violation; the caller then isolates the chunk
    name by name to find out which file is at fault.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise ArchiveError(f"не удалось прочитать ZIP-архив: {exc}") from exc

    with archive:
        infos = archive.infolist()

        names = [info.filename for info in infos]
        if len(names) != len(set(names)):
            raise ArchiveError("в архиве есть повторяющиеся имена записей")

        if set(names) != set(expected):
            missing = sorted(set(expected) - set(names))
            extra = sorted(set(names) - set(expected))
            raise ArchiveError(
                f"состав архива не совпадает с запросом: не хватает {missing}, лишние {extra}"
            )

        parsed: list[ParsedFile] = []
        for info in infos:
            if info.is_dir():
                raise ArchiveError(f"в архиве есть каталог: {info.filename!r}")
            _reject_path(info.filename)

            # Checked before extraction: the header is what protects us from a
            # decompression bomb.
            if info.file_size > max_entry_bytes:
                raise ArchiveError(
                    f"запись {info.filename!r} слишком велика: "
                    f"{info.file_size} байт при пределе {max_entry_bytes}"
                )

            raw = _read_entry(archive, info)
            try:
                text = raw.decode("ascii")
            except UnicodeDecodeError as exc:
                raise ArchiveError(f"запись {info.filename!r} не является ASCII") from exc

            content = text.rstrip("\r\n")
            if not CONTENT_PATTERN.fullmatch(content):
                raise ArchiveError(
                    f"содержимое {info.filename!r} не является {CONTENT_LENGTH} цифрами: "
                    f"длина {len(content)}"
                )

            parsed.append(
                ParsedFile(
                    name=info.filename,
                    content=content,
                    content_hash=content_hash(content),
                    digit_counts=digit_counts(content),
                )
            )

    return parsed
