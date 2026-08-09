"""SQLAlchemy models. Every timestamp is stored as timezone-aware UTC."""

from datetime import datetime

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    false,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.time import utc_now

# File content length, fixed by the task statement.
CONTENT_LENGTH = 500

RUN_STATUSES = ("pending", "starting", "running", "waiting_retry", "done", "failed")
EVENT_LEVELS = ("info", "warning", "error")

# Statuses that occupy the single download slot. Mirrors ACTIVE_STATUSES in
# app/services/runs.py; kept here as SQL because the database enforces the same
# rule through a partial unique index.
ACTIVE_STATUS_LIST = "'pending', 'starting', 'running', 'waiting_retry'"
ACTIVE_RUN_INDEX = "uq_download_run_active"


class Base(DeclarativeBase):
    pass


class DownloadRun(Base):
    """A single execution of the download process."""

    __tablename__ = "download_run"
    __table_args__ = (
        # `starting` sits between "claimed by a worker" and "provably holding the
        # download lock" (§ 8.8). Keep the list in sync with RUN_STATUSES and with
        # the migration that last changed this constraint.
        CheckConstraint(
            "status IN ('pending', 'starting', 'running', 'waiting_retry', 'done', 'failed')",
            name="ck_download_run_status",
        ),
        # At most one active run in the whole service, enforced by PostgreSQL
        # rather than by a check-then-insert (§ 7.1). The index is over a constant
        # expression, so two rows matching the WHERE clause collide with each
        # other whatever their statuses are. Created by migration 0003.
        Index(
            ACTIVE_RUN_INDEX,
            text("(status IS NOT NULL)"),
            unique=True,
            postgresql_where=text(f"status IN ({ACTIVE_STATUS_LIST})"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    candidate_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    names_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_saved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Which catalog this run talked to. Stored rather than inferred: a run against
    # the stub must be impossible to mistake for a real one afterwards — in the
    # log, in the list of runs, or in a screenshot.
    demo: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    # Diagnostics for status='failed': what happened, to which file, after how many attempts.
    error: Mapped[str | None] = mapped_column(Text)


class RunEvent(Base):
    """A log entry shown on the download page."""

    __tablename__ = "run_event"
    __table_args__ = (
        CheckConstraint("level IN ('info', 'warning', 'error')", name="ck_run_event_level"),
        Index("ix_run_event_run_id_id", "run_id", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("download_run.id", ondelete="CASCADE"), nullable=False
    )
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=func.now()
    )
    level: Mapped[str] = mapped_column(String(8), nullable=False)
    # User-facing text, kept in Russian on purpose — it is rendered in the UI as is.
    message: Mapped[str] = mapped_column(Text, nullable=False)


class File(Base):
    """A downloaded file together with its precomputed digit statistics.

    Counters live in separate d0..d9 columns rather than in JSONB so that the
    total over an entire selection is a plain SELECT sum(...), while the per-file
    table is paginated.
    """

    __tablename__ = "file"
    __table_args__ = (
        CheckConstraint(
            "d0 + d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 = 500",
            name="ck_file_digit_counts_sum",
        ),
        # Separate from the sum: (-1, 501, ...) also adds up to 500, and smallint
        # happily accepts negative values.
        CheckConstraint(
            " AND ".join(f"d{i} BETWEEN 0 AND 500" for i in range(10)),
            name="ck_file_digit_counts_range",
        ),
        UniqueConstraint("name", name="uq_file_name"),
        Index("ix_file_downloaded_at_id", "downloaded_at", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(CHAR(CONTENT_LENGTH), nullable=False)
    content_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    downloaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=func.now()
    )
    run_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("download_run.id", ondelete="SET NULL")
    )

    d0: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d1: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d2: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d3: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d4: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d5: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d6: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d7: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d8: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    d9: Mapped[int] = mapped_column(SmallInteger, nullable=False)


DIGIT_COLUMNS = (
    File.d0,
    File.d1,
    File.d2,
    File.d3,
    File.d4,
    File.d5,
    File.d6,
    File.d7,
    File.d8,
    File.d9,
)
