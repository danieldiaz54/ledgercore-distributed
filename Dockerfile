# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build
COPY pyproject.toml requirements.txt ./
COPY src/ ./src/
COPY ui/ ./ui/
COPY migrations/ ./migrations/

RUN python -m compileall -q src

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LEDGERCORE_ENV=production \
    LEDGERCORE_HOST=0.0.0.0 \
    LEDGERCORE_PORT=8096 \
    LEDGERCORE_SQLITE_PATH=/app/.data/ledgercore.sqlite3

RUN groupadd --system ledgercore && \
    useradd --system --gid ledgercore --create-home --home-dir /app ledgercore && \
    mkdir -p /app/.data && \
    chown -R ledgercore:ledgercore /app

WORKDIR /app
COPY --from=builder --chown=ledgercore:ledgercore /build/pyproject.toml /build/requirements.txt ./
COPY --from=builder --chown=ledgercore:ledgercore /build/src ./src
COPY --from=builder --chown=ledgercore:ledgercore /build/ui ./ui
COPY --from=builder --chown=ledgercore:ledgercore /build/migrations ./migrations

USER ledgercore

EXPOSE 8096

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8096/api/v1/health', timeout=3)" || exit 1

CMD ["python", "-m", "src.main"]
