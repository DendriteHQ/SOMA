FROM python:3.11-slim

WORKDIR /sandbox

COPY requirements.txt /sandbox/requirements.txt
RUN pip install --no-cache-dir -r /sandbox/requirements.txt
ENV TIKTOKEN_CACHE_DIR=/tiktoken_cache \
    NLTK_DATA=/usr/local/share/nltk_data
RUN mkdir -p /tiktoken_cache "${NLTK_DATA}" \
    && python -m nltk.downloader -d "${NLTK_DATA}" stopwords punkt punkt_tab \
    && python -c "import tiktoken; tiktoken.get_encoding('cl100k_base');" \
    && chmod -R 755 /tiktoken_cache "${NLTK_DATA}"
COPY run_code.py /sandbox/run_code.py

CMD ["python", "/sandbox/run_code.py"]
