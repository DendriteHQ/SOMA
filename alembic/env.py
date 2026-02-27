import os
import sys
from urllib.parse import quote
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
APP_DIR = os.path.join(ROOT_DIR, "mcp_platform")
if APP_DIR not in sys.path:
    sys.path.append(APP_DIR)

load_dotenv(dotenv_path=os.path.join(APP_DIR, ".env"))

from app.core.config import settings
from app.db.models.base import Base

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def _get_url() -> str:
    if settings.rds_secret_id:
        secret = settings._read_rds_secret()
        host = settings._resolve_rds_host(secret)
        if not host:
            raise RuntimeError("RDS secret is missing host information")
        user = secret.get("username")
        password = secret.get("password")
        if not user or not password:
            raise RuntimeError("RDS secret is missing username or password")
        db_name = (
            settings.rds_db_name
            or secret.get("dbname")
            or secret.get("db_name")
            or secret.get("database")
        )
        if not db_name:
            raise RuntimeError("RDS secret is missing database name")
        port = settings.rds_port or secret.get("port") or 5432
        engine = str(secret.get("engine") or "postgres").lower()
        scheme = "postgresql+asyncpg" if "postgres" in engine else "postgresql+asyncpg"
        url = (
            f"{scheme}://{quote(str(user))}:{quote(str(password))}"
            f"@{host}:{port}/{quote(str(db_name))}"
        )
    else:
        url = settings.get_postgres_dsn()
    if not url:
        raise RuntimeError("POSTGRES_DSN or RDS_SECRET_ID must be set")
    return url.replace("postgresql+asyncpg", "postgresql+psycopg2")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url = _get_url()
    # Alembic uses ConfigParser interpolation; escape % chars in URLs.
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
