"""Link top_miners to miners and scripts.

Revision ID: 6f3f1e2d9a7b
Revises: ba72601778ea
Create Date: 2026-02-20
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "6f3f1e2d9a7b"
down_revision = "ba72601778ea"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "top_miners",
        sa.Column("miner_fk", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "top_miners",
        sa.Column("script_fk", sa.BigInteger(), nullable=True),
    )
    op.create_index("ix_top_miners_miner_fk", "top_miners", ["miner_fk"])
    op.create_foreign_key(
        "fk_top_miners_miner",
        "top_miners",
        "miners",
        ["miner_fk"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_top_miners_script",
        "top_miners",
        "scripts",
        ["script_fk"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_top_miners_script", "top_miners", type_="foreignkey")
    op.drop_constraint("fk_top_miners_miner", "top_miners", type_="foreignkey")
    op.drop_index("ix_top_miners_miner_fk", table_name="top_miners")
    op.drop_column("top_miners", "script_fk")
    op.drop_column("top_miners", "miner_fk")
