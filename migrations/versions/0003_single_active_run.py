"""Guarantee at most one active run, in the database.

Until now "one run at a time" was a check followed by an insert, and two
simultaneous `POST /api/runs` could both pass the check and create a run. The
Redis lock did not save this: with `--concurrency 1` the worker runs the tasks
one after another, so the second run finds the lock free by the time it starts
and goes to the external API — exactly the thing the single-run rule exists to
prevent (§ 7.1).

The index is over a constant expression (`status IS NOT NULL`, true for every
row) restricted to the active statuses, so any two active rows collide with each
other regardless of their individual statuses.

Runs already active when this migration runs are reconciled first: the oldest is
kept — that is the one the application itself treats as "the" active run — and
the rest are closed as `failed` with a reason. Terminal runs are never touched.

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX = "uq_download_run_active"
TABLE = "download_run"
ACTIVE = "'pending', 'starting', 'running', 'waiting_retry'"

REASON = (
    "ран закрыт при установке ограничения «один активный ран»: "
    "одновременно активными числились несколько ранов"
)


def upgrade() -> None:
    # Deterministic and narrow: keep MIN(id) among the active runs — the same one
    # find_active_run() reports — and close every other active row. A run that is
    # already done or failed keeps its outcome.
    op.execute(
        f"""
        UPDATE download_run
        SET status = 'failed',
            finished_at = COALESCE(finished_at, now()),
            error = COALESCE(error, '{REASON}')
        WHERE status IN ({ACTIVE})
          AND id > (SELECT MIN(id) FROM download_run WHERE status IN ({ACTIVE}))
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX {INDEX}
        ON {TABLE} ((status IS NOT NULL))
        WHERE status IN ({ACTIVE})
        """
    )


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {INDEX}")
