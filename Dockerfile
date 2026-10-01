# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------- #
# Base: runtime dependencies only. Split from the app layer so a code change
# does not reinstall pyarrow and pandas on every build.
# ---------------------------------------------------------------------------- #
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# tzdata: the scheduler works in US/Eastern, and the exchange calendar needs
# zoneinfo data. ca-certificates: HTTPS to the data provider.
RUN apt-get update \
 && apt-get install --no-install-recommends -y tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

# ---------------------------------------------------------------------------- #
# App: the collector itself, running as a non-root user.
# ---------------------------------------------------------------------------- #
FROM base AS app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps -e .

# UID/GID are build args so they can be matched to the Unraid share owner
# (nobody:users = 99:100 by default); a mismatch shows up as permission errors
# on /data rather than anything subtler.
ARG APP_UID=99
ARG APP_GID=100
RUN groupadd -g "${APP_GID}" -o collector \
 && useradd -u "${APP_UID}" -g "${APP_GID}" -o -M -d /app -s /usr/sbin/nologin collector \
 && mkdir -p /data \
 && chown -R "${APP_UID}:${APP_GID}" /app /data

COPY config ./config

USER collector
ENV COLLECTOR_CONFIG=/app/config/collector.toml \
    COLLECTOR_DATA_ROOT=/data
VOLUME ["/data"]

# Unhealthy if status.json is stale or the last run hard-failed. start-period is
# generous because the first run of the day can take a while.
HEALTHCHECK --interval=5m --timeout=30s --start-period=2m --retries=3 \
    CMD ["python", "-m", "collector", "healthcheck"]

ENTRYPOINT ["python", "-m", "collector"]
CMD ["serve"]

# ---------------------------------------------------------------------------- #
# Dev: adds the test and lint tooling. Not used in deployment.
# ---------------------------------------------------------------------------- #
FROM base AS dev

COPY requirements-dev.txt ./
RUN pip install -r requirements-dev.txt
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps -e .
COPY config ./config
COPY tests ./tests
# The deployment tests assert on these, so they belong in the image rather than
# depending on how the test runner happens to mount things.
COPY compose.yaml compose.dev.yaml .env.example ./
ENV COLLECTOR_CONFIG=/app/config/collector.toml \
    COLLECTOR_DATA_ROOT=/data
ENTRYPOINT []
CMD ["pytest"]
