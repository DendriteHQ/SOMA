FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/soma/mcp_platform_services/run-coordinator:/opt/soma/mcp_platform_services/common:/opt/soma TIKTOKEN_CACHE_DIR=/opt/tiktoken
RUN apt-get update && apt-get install -y --no-install-recommends awscli ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/soma
COPY mcp_platform_services/common/requirements.txt /tmp/requirements.txt
COPY --from=soma_shared pyproject.toml README.md /opt/soma-shared/
COPY --from=soma_shared soma_shared /opt/soma-shared/soma_shared
RUN pip install -r /tmp/requirements.txt /opt/soma-shared
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"
COPY mcp_platform_services/common/app /opt/soma/mcp_platform_services/common/app
COPY mcp_platform_services/run-coordinator/app /opt/soma/mcp_platform_services/run-coordinator/app
RUN useradd --uid 10001 --create-home soma && mkdir -p /var/lib/soma \
    && chown -R soma:soma /var/lib/soma
USER soma
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=3)"
ENV SOMA_SERVICE_ROLE=coordinator DB_INITIALIZE_SCHEMA=false
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-proxy-headers"]
