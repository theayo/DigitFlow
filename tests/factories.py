"""Row builders shared by the API tests."""

from datetime import datetime

from app.models import CONTENT_LENGTH, File
from app.worker.archive import content_hash, digit_counts


def build_file(
    name: str,
    downloaded_at: datetime,
    *,
    content: str | None = None,
    run_id: int | None = None,
) -> File:
    """A valid file row: counters and hash derived from the content itself.

    Deriving them keeps the fixtures inside the invariant the database enforces
    (`sum(d0..d9) = 500`, `content_hash = sha256(content)`), so a test never has
    to hand-maintain eleven consistent numbers.
    """
    body = content if content is not None else "0" * CONTENT_LENGTH
    assert len(body) == CONTENT_LENGTH, "содержимое файла — ровно 500 цифр"
    counts = digit_counts(body)
    return File(
        name=name,
        content=body,
        content_hash=content_hash(body),
        downloaded_at=downloaded_at,
        run_id=run_id,
        **{f"d{digit}": counts[digit] for digit in range(10)},
    )
