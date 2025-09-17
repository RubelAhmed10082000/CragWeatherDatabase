# api.Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src/arquivio/api/ /app/src/arquivio/api/

ENV PYTHONPATH=/app

EXPOSE 8080
ENV PORT=8080

CMD ["uvicorn", "src.arquivio.api.main:app", "--host=0.0.0.0", "--port=8080", "--workers=2"]


