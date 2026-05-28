FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Pre-cache the cl100k_base tiktoken encoding so the miner can run offline.
# tiktoken looks up encodings by SHA256 in TIKTOKEN_CACHE_DIR (default ~/.tiktoken).
RUN python - <<'EOF'
import tiktoken
tiktoken.get_encoding("cl100k_base")
print("cl100k_base cached")
EOF

# Copy the FastAPI application
COPY app/ ./app/

# Create the directory where base_miner.py will be volume-mounted at runtime
RUN mkdir -p /app/miner

ENV MINER_MODULE_PATH=/app/miner/base_miner.py
ENV LOG_LEVEL=INFO
ENV PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=6 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${PORT}/health', timeout=2)" || exit 1

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
