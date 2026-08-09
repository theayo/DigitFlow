"""Remember whether a run used the demo catalog.

The download page can be shown against the stub instead of the real service
(§ 7.1), and afterwards nothing else would tell the two apart: the log, the file
list and a screenshot look identical. The flag is stored on the run so the
answer stays with the row that it describes.

Existing runs predate the switch and all went to the real service, hence the
`false` default.

Revision ID: 0004
Revises: 0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "download_run",
        sa.Column("demo", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("download_run", "demo")
