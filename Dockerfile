# Using Python 3.13 as a base
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update -y && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Installing deps
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . /app
CMD ["python", "-m", "jobs.weather_updater.run_shard"]

RUN useradd -m appuser
USER appuser

COPY --chown=appuser:appuser . /app



