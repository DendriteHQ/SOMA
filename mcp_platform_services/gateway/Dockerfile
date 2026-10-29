FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends awscli ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/soma
COPY mcp_platform_services/gateway/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY mcp_platform_services/gateway/gateway /opt/soma/gateway
RUN useradd --uid 10001 --create-home soma && mkdir -p /var/lib/soma \
    && chown -R soma:soma /var/lib/soma
USER soma
EXPOSE 8050
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8050/health/ready', timeout=3)"
CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8050", "--workers", "1", "--no-proxy-headers"]
