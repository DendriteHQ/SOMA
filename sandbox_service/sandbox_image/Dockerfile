FROM python:3.11-slim

WORKDIR /sandbox

COPY requirements.txt /sandbox/requirements.txt
RUN pip install --no-cache-dir -r /sandbox/requirements.txt
ENV TIKTOKEN_CACHE_DIR=/tiktoken_cache
RUN mkdir -p /tiktoken_cache
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base');" \
    && chmod -R 755 /tiktoken_cache
COPY run_code.py /sandbox/run_code.py

ENTRYPOINT ["python", "/sandbox/run_code.py"]