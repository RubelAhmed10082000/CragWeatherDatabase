FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update -y && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# install deps
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# create non-root user before copying code; copy with ownership once
RUN useradd -m appuser
USER appuser
COPY --chown=appuser:appuser . /app

# default command -> upsert runner
ENTRYPOINT ["python","-m","jobs.weather_updater.upsert.run_upsert"]



