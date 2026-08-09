"""Add the `starting` run status.

`starting` separates "a worker has claimed this run" from "this run provably
holds the download lock" (§ 8.8). Without it a run appears as `running` before
the lock is taken, and orphan reaping — which treats a lock-less `running` run as
dead — can close a run that is merely mid-startup.

Only the status check constraint changes; no data migration is needed going
forward. The downgrade has to deal with rows the old constraint would reject, so
it closes any run still in `starting` as `failed`: such a run never began working
and cannot be resumed, since nothing about its progress is persisted.

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT = "ck_download_run_status"
TABLE = "download_run"

OLD_STATUSES = "'pending', 'running', 'waiting_retry', 'done', 'failed'"
NEW_STATUSES = "'pending', 'starting', 'running', 'waiting_retry', 'done', 'failed'"


def upgrade() -> None:
    op.drop_constraint(CONSTRAINT, TABLE, type_="check")
    op.create_check_constraint(CONSTRAINT, TABLE, f"status IN ({NEW_STATUSES})")


def downgrade() -> None:
    op.execute(
        """
        UPDATE download_run
        SET status = 'failed',
            finished_at = COALESCE(finished_at, now()),
            error = COALESCE(
                error,
                'ран закрыт при откате миграции: статус starting больше не поддерживается'
            )
        WHERE status = 'starting'
        """
    )
    op.drop_constraint(CONSTRAINT, TABLE, type_="check")
    op.create_check_constraint(CONSTRAINT, TABLE, f"status IN ({OLD_STATUSES})")
