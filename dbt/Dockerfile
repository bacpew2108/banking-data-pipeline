FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# dbt-postgres để chạy ELT transformations
RUN pip install --no-cache-dir dbt-postgres==1.7.13

# Thư viện cho staging script (load Parquet từ MinIO vào PostgreSQL)
RUN pip install --no-cache-dir \
    boto3==1.35.0 \
    pyarrow==17.0.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.2.2

WORKDIR /usr/app/dbt
ENV DBT_PROFILES_DIR=/usr/app/dbt

# Không dùng ENTRYPOINT cố định nữa, để Airflow có thể exec cả python và dbt
CMD ["tail", "-f", "/dev/null"]
