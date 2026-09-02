"""tabelas transactions e outbox

Revision ID: 0001
Revises:
Create Date: 2026-09-01
"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("customer_id", sa.String(64), nullable=False),
        sa.Column("value", sa.Numeric(12, 2), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_transactions_customer_id", "transactions", ["customer_id"])
    op.create_index("ix_transactions_status", "transactions", ["status"])
    op.create_table(
        "outbox",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("aggregate_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(), nullable=False),
        sa.Column("published_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"])
    op.create_index("ix_outbox_published_at", "outbox", ["published_at"])


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("transactions")
