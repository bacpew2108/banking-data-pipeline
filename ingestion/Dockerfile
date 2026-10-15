FROM python:3.11-slim

# Cài đặt unixODBC và MS ODBC Driver 18 cho Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir pyodbc python-dotenv

# Chỉ chép code ingestion (+ dữ liệu nguồn), không chép cả dự án (.venv, .env...).
# .env được mount lúc chạy (docker-compose), không nằm trong image.
COPY ingestion/ ingestion/

ENV DB_SERVER=sqlserver
CMD ["python", "ingestion/replay_transactions.py"]
