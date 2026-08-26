from __future__ import annotations

import json
import os
import subprocess

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote


_DEFAULT_ENV_FILE = Path(__file__).resolve().parents[1] / "mcp_platform" / ".env"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {"'", '"'}
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _read_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _read_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _read_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    env_file: Path
    metadata_dir: Path
    log_level: str
    postgres_dsn: str | None
    rds_secret_id: str | None
    rds_writer_host: str | None
    rds_port: int | None
    rds_db_name: str | None
    db_pool_size: int
    db_max_overflow: int
    interval_seconds: float
    min_run_age_seconds: float
    batch_size: int
    fetch_limit: int
    advisory_lock_key: int
    dry_run: bool

    @classmethod
    def from_env(cls) -> "Settings":
        env_file = Path(
            os.getenv("AUTO_RUN_RESTARTER_ENV_FILE", str(_DEFAULT_ENV_FILE))
        )
        _load_env_file(env_file)

        batch_size = max(1, _read_int("AUTO_RUN_RESTARTER_BATCH_SIZE", 100))
        fetch_limit = max(
            batch_size,
            _read_int("AUTO_RUN_RESTARTER_FETCH_LIMIT", batch_size * 20),
        )

        return cls(
            env_file=env_file,
            metadata_dir=Path(
                os.getenv(
                    "AUTO_RUN_RESTARTER_METADATA_DIR",
                    str(Path(__file__).resolve().parent / "metadata"),
                )
            ),
            log_level=(os.getenv("AUTO_RUN_RESTARTER_LOG_LEVEL") or "INFO").upper(),
            postgres_dsn=os.getenv("POSTGRES_DSN"),
            rds_secret_id=os.getenv("RDS_SECRET_ID"),
            rds_writer_host=os.getenv("RDS_WRITER_HOST"),
            rds_port=_read_int("RDS_PORT", 5432) if os.getenv("RDS_PORT") else None,
            rds_db_name=os.getenv("RDS_DB_NAME"),
            db_pool_size=max(1, _read_int("AUTO_RUN_RESTARTER_DB_POOL_SIZE", 2)),
            db_max_overflow=max(
                0, _read_int("AUTO_RUN_RESTARTER_DB_MAX_OVERFLOW", 2)
            ),
            interval_seconds=max(
                5.0, _read_float("AUTO_RUN_RESTARTER_INTERVAL_SECONDS", 60.0)
            ),
            min_run_age_seconds=max(
                30.0, _read_float("AUTO_RUN_RESTARTER_MIN_RUN_AGE_SECONDS", 120.0)
            ),
            batch_size=batch_size,
            fetch_limit=fetch_limit,
            advisory_lock_key=_read_int(
                "AUTO_RUN_RESTARTER_ADVISORY_LOCK_KEY",
                11426001,
            ),
            dry_run=_read_bool("AUTO_RUN_RESTARTER_DRY_RUN", False),
        )

    def _read_rds_secret(self) -> dict[str, object]:
        if not self.rds_secret_id:
            raise RuntimeError("RDS_SECRET_ID is required to load the RDS secret")
        cmd = [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            self.rds_secret_id,
            "--query",
            "SecretString",
            "--output",
            "text",
        ]
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        try:
            return json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            raise RuntimeError("RDS secret is not valid JSON") from exc

    def get_postgres_writer_dsn(self) -> str:
        if self.postgres_dsn:
            return self.postgres_dsn

        secret = self._read_rds_secret()
        host = self.rds_writer_host or str(
            secret.get("host") or secret.get("hostname") or ""
        ).strip()
        if not host:
            raise RuntimeError("RDS writer host is missing")

        user = secret.get("username")
        password = secret.get("password")
        if not user or not password:
            raise RuntimeError("RDS secret is missing username or password")

        db_name = self.rds_db_name or str(
            secret.get("dbname")
            or secret.get("db_name")
            or secret.get("database")
            or ""
        ).strip()
        if not db_name:
            raise RuntimeError("RDS secret is missing database name")

        port = self.rds_port or int(secret.get("port") or 5432)
        return (
            f"postgresql+asyncpg://{quote(str(user))}:{quote(str(password))}"
            f"@{host}:{port}/{quote(str(db_name))}"
        )
