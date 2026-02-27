"""apply cascade rules

Revision ID: ba72601778ea
Revises: c2c3f9a1b7d4
Create Date: 2026-02-18 12:21:20.318338

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ba72601778ea"
down_revision: Union[str, Sequence[str], None] = "c2c3f9a1b7d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


FOREIGN_KEYS: list[tuple[str, str, str, list[str], list[str], str | None]] = [
    ("answers", "fk_answers_question", "questions", ["question_fk"], ["id"], "CASCADE"),
    (
        "batch_assignments",
        "fk_ba_batch",
        "challenge_batches",
        ["challenge_batch_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_challenges",
        "fk_batchch_batch",
        "challenge_batches",
        ["challenge_batch_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_challenges",
        "fk_batchch_challenge",
        "challenges",
        ["challenge_fk"],
        ["id"],
        "RESTRICT",
    ),
    (
        "batch_challenge_scores",
        "fk_bcs_batchch",
        "batch_challenges",
        ["batch_challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_question_answers",
        "fk_bqa_batchch",
        "batch_challenges",
        ["batch_challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_question_answers",
        "fk_bqa_question",
        "questions",
        ["question_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_question_scores",
        "fk_bqs_batchch",
        "batch_challenges",
        ["batch_challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "batch_question_scores",
        "fk_bqs_question",
        "questions",
        ["question_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "burn_requests",
        "fk_br_request",
        "requests",
        ["request_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "competition_configs",
        "fk_comp_configs_comp",
        "competitions",
        ["competition_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "competition_timeframes",
        "fk_comp_timeframes_config",
        "competition_configs",
        ["competition_config_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "competition_challenges",
        "fk_compch_comp",
        "competitions",
        ["competition_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "competition_challenges",
        "fk_compch_challenge",
        "challenges",
        ["challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "compression_competition_config",
        "fk_compression_config",
        "competition_configs",
        ["competition_config_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "exception_logs",
        "fk_el_request",
        "requests",
        ["request_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "questions",
        "fk_questions_challenge",
        "challenges",
        ["challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "screeners",
        "fk_screeners_comp",
        "competitions",
        ["competition_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "screening_challenges",
        "fk_scrch_screener",
        "screeners",
        ["screener_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "screening_challenges",
        "fk_scrch_challenge",
        "challenges",
        ["challenge_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "signed_requests",
        "fk_sr_request",
        "requests",
        ["request_fk"],
        ["id"],
        "CASCADE",
    ),
    (
        "scripts",
        "scripts_request_fk_fkey",
        "requests",
        ["request_fk"],
        ["id"],
        "SET NULL",
    ),
    (
        "validator_heartbeats",
        "validator_heartbeats_request_fk_fkey",
        "requests",
        ["request_fk"],
        ["id"],
        "SET NULL",
    ),
    (
        "validator_registrations",
        "validator_registrations_request_fk_fkey",
        "requests",
        ["request_fk"],
        ["id"],
        "SET NULL",
    ),
    (
        "miner_uploads",
        "fk_uploads_comp",
        "competitions",
        ["competition_fk"],
        ["id"],
        "SET NULL",
    ),
]


def _drop_fk(
    table: str,
    constraint: str,
    cols: list[str],
) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for fk in inspector.get_foreign_keys(table):
        fk_name = fk.get("name")
        if fk_name == constraint or fk.get("constrained_columns") == cols:
            op.drop_constraint(fk_name, table, type_="foreignkey")


def upgrade() -> None:
    """Upgrade schema."""
    for (
        table,
        constraint,
        _ref_table,
        local_cols,
        _remote_cols,
        _ondelete,
    ) in FOREIGN_KEYS:
        _drop_fk(table, constraint, local_cols)

    op.alter_column(
        "miner_uploads",
        "competition_fk",
        existing_type=sa.BigInteger(),
        nullable=True,
    )

    op.alter_column(
        "validator_registrations",
        "request_fk",
        existing_type=sa.BigInteger(),
        nullable=True,
    )

    for table, constraint, ref_table, local_cols, remote_cols, ondelete in FOREIGN_KEYS:
        op.create_foreign_key(
            constraint,
            table,
            ref_table,
            local_cols,
            remote_cols,
            ondelete=ondelete,
        )


def downgrade() -> None:
    """Downgrade schema."""
    for (
        table,
        constraint,
        _ref_table,
        local_cols,
        _remote_cols,
        _ondelete,
    ) in FOREIGN_KEYS:
        _drop_fk(table, constraint, local_cols)

    op.alter_column(
        "miner_uploads",
        "competition_fk",
        existing_type=sa.BigInteger(),
        nullable=False,
    )

    op.alter_column(
        "validator_registrations",
        "request_fk",
        existing_type=sa.BigInteger(),
        nullable=False,
    )

    for table, constraint, ref_table, local_cols, remote_cols, _ in FOREIGN_KEYS:
        op.create_foreign_key(
            constraint,
            table,
            ref_table,
            local_cols,
            remote_cols,
        )
