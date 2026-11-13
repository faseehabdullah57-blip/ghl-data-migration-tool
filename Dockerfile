FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY data/ ./data/
COPY src/ ./src/
COPY README.md .

RUN mkdir -p /app/data/output

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--dry-run"]
