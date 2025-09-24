"""${message}

Revisão: ${up_revision}
Reverte: ${down_revision | comma,n}
Criado em: ${create_date}
"""

from alembic import op
import sqlalchemy as sa
import json

# Revisão
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

# ======================================
# Helpers robustos para migrations
# ======================================
def _safe_add_column(table: str, column: sa.Column):
    """Adiciona coluna apenas se não existir (idempotência)."""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c["name"] for c in insp.get_columns(table)]
    if column.name not in cols:
        op.add_column(table, column)


def _safe_drop_column(table: str, col_name: str):
    """Remove coluna apenas se existir (idempotência)."""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c["name"] for c in insp.get_columns(table)]
    if col_name in cols:
        op.drop_column(table, col_name)


def _safe_create_index(name: str, table: str, cols: list, unique=False):
    """Cria índice apenas se não existir."""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    indexes = [ix["name"] for ix in insp.get_indexes(table)]
    if name not in indexes:
        op.create_index(name, table, cols, unique=unique)


# ======================================
# Upgrade / Downgrade
# ======================================
def upgrade():
    """Implementa a migração avançada."""
    # Exemplo: adiciona coluna de tracking se não existir
    _safe_add_column("buyers", sa.Column("session_metadata", sa.JSON(), nullable=True))
    _safe_add_column("buyers", sa.Column("device_info", sa.JSON(), nullable=True))

    # Índices estratégicos
    _safe_create_index("ix_buyers_event_key", "buyers", ["event_key"], unique=True)
    _safe_create_index("ix_buyers_created_at", "buyers", ["created_at"])


def downgrade():
    """Reverte a migração (rollback seguro)."""
    _safe_drop_column("buyers", "session_metadata")
    _safe_drop_column("buyers", "device_info")