"""Initial schema: runs, log events, files.

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "download_run",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("candidate_id", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("names_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("files_saved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'waiting_retry', 'done', 'failed')",
            name="ck_download_run_status",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_download_run_status", "download_run", ["status"])

    op.create_table(
        "run_event",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("level", sa.String(length=8), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.CheckConstraint("level IN ('info', 'warning', 'error')", name="ck_run_event_level"),
        sa.ForeignKeyConstraint(["run_id"], ["download_run.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_run_event_run_id_id", "run_event", ["run_id", "id"])

    op.create_table(
        "file",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("content", sa.CHAR(length=500), nullable=False),
        sa.Column("content_hash", sa.CHAR(length=64), nullable=False),
        sa.Column(
            "downloaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("run_id", sa.BigInteger(), nullable=True),
        *[sa.Column(f"d{i}", sa.SmallInteger(), nullable=False) for i in range(10)],
        sa.CheckConstraint(
            "d0 + d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 = 500",
            name="ck_file_digit_counts_sum",
        ),
        sa.CheckConstraint(
            " AND ".join(f"d{i} BETWEEN 0 AND 500" for i in range(10)),
            name="ck_file_digit_counts_range",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["download_run.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_file_name"),
    )
    op.create_index("ix_file_downloaded_at_id", "file", ["downloaded_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_file_downloaded_at_id", table_name="file")
    op.drop_table("file")
    op.drop_index("ix_run_event_run_id_id", table_name="run_event")
    op.drop_table("run_event")
    op.drop_index("ix_download_run_status", table_name="download_run")
    op.drop_table("download_run")
