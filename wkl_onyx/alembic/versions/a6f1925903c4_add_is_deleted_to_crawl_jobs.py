"""add_is_deleted_to_crawl_jobs

Revision ID: a6f1925903c4
Revises: 174e3d785bd0
Create Date: 2026-04-09 19:24:57.604732

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a6f1925903c4'
down_revision: Union[str, Sequence[str], None] = '174e3d785bd0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('crawl_jobs', sa.Column('is_deleted', sa.Boolean(), nullable=True))
    op.execute("UPDATE crawl_jobs SET is_deleted = FALSE")
    op.alter_column('crawl_jobs', 'is_deleted', nullable=False, server_default=sa.false())

def downgrade() -> None:
    op.drop_column('crawl_jobs', 'is_deleted')