FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-m", "src.etl_process"]
