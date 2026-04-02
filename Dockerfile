FROM python:3.13-slim

WORKDIR /app

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update -o Acquire::Retries=3 && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir "asyncpg>=0.30.0" rich==13.7.1

COPY . .

CMD ["python", "main.py"]
