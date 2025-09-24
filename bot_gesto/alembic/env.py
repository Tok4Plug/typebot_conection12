# ========================================
# alembic/env.py - versão avançada assíncrona
# ========================================

import os
import sys
import asyncio
from logging.config import fileConfig
from sqlalchemy.ext.asyncio import create_async_engine
from alembic import context

# ========================================
# Ajusta path para garantir que o db.py seja encontrado
# ========================================
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ---- Carrega .env se existir
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
except Exception:
    pass

# Configuração do Alembic
config = context.config

# Logging do Alembic
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Importa metadata da aplicação
from db import Base  # <- Base declarative_base() do db.py
target_metadata = Base.metadata

# ========================================
# URL do banco (assíncrono com asyncpg)
# ========================================
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRESQL_URL")
)

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL não definida no .env ou variáveis do sistema.")

# Garante driver assíncrono
if not DATABASE_URL.startswith("postgresql+asyncpg"):
    raise RuntimeError(
        "❌ DATABASE_URL precisa usar o driver assíncrono:\n"
        "Exemplo: postgresql+asyncpg://user:pass@host:5432/dbname"
    )

# Injeta no config
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# ========================================
# Funções de migração
# ========================================

def run_migrations_offline():
    """Executa migrações no modo offline (gera apenas SQL)."""
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,              # compara tipos de colunas
        compare_server_default=True,    # compara defaults
        render_as_batch=True,           # batch mode (necessário p/ SQLite)
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    """Executa migrações no modo online (usando asyncpg)."""
    connectable = create_async_engine(DATABASE_URL, future=True, echo=False)

    async with connectable.connect() as connection:
        await connection.run_sync(
            lambda sync_conn: context.configure(
                connection=sync_conn,
                target_metadata=target_metadata,
                compare_type=True,
                compare_server_default=True,
                render_as_batch=True,
            )
        )

        async with context.begin_transaction():
            await context.run_migrations()

    await connectable.dispose()


# ========================================
# Entrada principal
# ========================================
if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())