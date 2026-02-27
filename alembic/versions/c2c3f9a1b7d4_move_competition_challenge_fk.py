"""Move competition challenge FK to competition_challenges.

Revision ID: c2c3f9a1b7d4
Revises:
Create Date: 2026-02-16
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c2c3f9a1b7d4"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "competition_challenges",
        sa.Column("challenge_fk", sa.BigInteger(), nullable=True),
    )

    op.execute("""
        INSERT INTO competition_challenges (competition_fk, challenge_fk, is_active)
        SELECT
            cc.competition_fk,
            c.id AS challenge_fk,
            cc.is_active
        FROM challenges c
        JOIN competition_challenges cc
          ON c.competition_challenge_fk = cc.id
        WHERE c.competition_challenge_fk IS NOT NULL
        """)

    op.execute(
        "ALTER TABLE challenges "
        "DROP CONSTRAINT IF EXISTS challenges_competition_challenge_fk_fkey"
    )
    op.drop_column("challenges", "competition_challenge_fk")

    op.execute("DELETE FROM competition_challenges WHERE challenge_fk IS NULL")

    op.alter_column("competition_challenges", "challenge_fk", nullable=False)

    op.create_foreign_key(
        "competition_challenges_challenge_fk_fkey",
        "competition_challenges",
        "challenges",
        ["challenge_fk"],
        ["id"],
    )


def downgrade() -> None:
    op.add_column(
        "challenges",
        sa.Column("competition_challenge_fk", sa.BigInteger(), nullable=True),
    )

    op.execute("""
        UPDATE challenges c
        SET competition_challenge_fk = cc.id
        FROM competition_challenges cc
        WHERE cc.challenge_fk = c.id
        """)

    op.create_foreign_key(
        "challenges_competition_challenge_fk_fkey",
        "challenges",
        "competition_challenges",
        ["competition_challenge_fk"],
        ["id"],
    )

    op.drop_constraint(
        "competition_challenges_challenge_fk_fkey",
        "competition_challenges",
        type_="foreignkey",
    )
    op.drop_column("competition_challenges", "challenge_fk")
