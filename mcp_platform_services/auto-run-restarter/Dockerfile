FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/soma TIKTOKEN_CACHE_DIR=/opt/tiktoken \
    AUTO_RUN_RESTARTER_METADATA_DIR=/var/lib/soma/restarts
RUN apt-get update && apt-get install -y --no-install-recommends awscli ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/soma
COPY mcp_platform_services/common/requirements.txt /tmp/requirements.txt
COPY --from=soma_shared pyproject.toml README.md /opt/soma-shared/
COPY --from=soma_shared soma_shared /opt/soma-shared/soma_shared
RUN pip install -r /tmp/requirements.txt /opt/soma-shared
COPY mcp_platform_services/auto-run-restarter/auto_run_restarter /opt/soma/auto_run_restarter
RUN useradd --uid 10001 --create-home soma && mkdir -p /var/lib/soma/restarts \
    && chown -R soma:soma /var/lib/soma
USER soma
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python -c "from auto_run_restarter.settings import Settings; from pathlib import Path; import time; s = Settings.from_env(); p = s.metadata_dir / 'last_successful_tick'; raise SystemExit(0 if p.exists() and time.time() - p.stat().st_mtime <= max(180, s.interval_seconds * 3 + 30) else 1)"
CMD ["python", "-m", "auto_run_restarter"]
