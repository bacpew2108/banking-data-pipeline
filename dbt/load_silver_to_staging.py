"""
load_silver_to_staging.py
Nạp dữ liệu Silver Parquet từ MinIO vào PostgreSQL schema 'staging'
để dbt có thể truy vấn và chuyển đổi sang Gold (Star Schema).

Sử dụng:
  - boto3 / s3fs để đọc Parquet từ MinIO
  - pyarrow để đọc file Parquet
  - psycopg2 + COPY để nạp nhanh vào PostgreSQL

Chạy: python /usr/app/dbt/load_silver_to_staging.py
"""

import os
import sys
import io
import csv
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import psycopg2
from psycopg2 import sql


# ── 1. Đọc cấu hình từ .env ──
def load_env():
    candidates = [
        Path("/usr/app/dbt/.env"),
        Path("/opt/spark/.env"),
        Path(__file__).resolve().parent / ".env",
        Path(".env"),
    ]
    for env_path in candidates:
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip().strip("'\""))
            print(f"✓ Đã nạp biến môi trường từ {env_path}")
            break

load_env()


# ── 2. Cấu hình kết nối (bắt buộc khai báo trong .env, không dùng mật khẩu mặc định) ──
def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Lỗi: Thiếu biến môi trường {name} trong .env!")
    return value


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = require_env("MINIO_ROOT_USER")
MINIO_SECRET_KEY = require_env("MINIO_ROOT_PASSWORD")

PG_HOST = os.environ.get("PG_DWH_HOST", "data_warehouse")
PG_PORT = os.environ.get("PG_DWH_PORT", "5432")
PG_USER = require_env("POSTGRES_USER")
PG_PASSWORD = require_env("POSTGRES_PASSWORD")
PG_DB = require_env("POSTGRES_DB")

STAGING_SCHEMA = "staging"

# Mapping Silver path -> Staging table + cột cần nạp (tên cột -> kiểu PostgreSQL).
# Cột mới thêm vào đây sẽ tự được ALTER TABLE ADD COLUMN trên bảng staging đã tồn tại.
# Cột partition (year/month/day) không nằm trong file Parquet mà nằm trên đường dẫn
# key=value, nên được lấy từ đường dẫn file (xem partition_values).
TABLES_CONFIG = {
    "stg_transactions": {
        "bucket": "silver",
        "prefix": "transactions_clean/",
        "columns": {
            "id": "INTEGER",
            "date": "BIGINT",
            "client_id": "INTEGER",
            "card_id": "INTEGER",
            "amount": "DECIMAL(10,2)",
            "use_chip": "VARCHAR(50)",
            "merchant_id": "INTEGER",
            "merchant_city": "VARCHAR(100)",
            "merchant_state": "VARCHAR(50)",
            "zip": "VARCHAR(20)",
            "mcc": "INTEGER",
            "errors": "TEXT",
            "tx_timestamp": "TIMESTAMP",
            "is_error": "BOOLEAN",
            "is_refund": "BOOLEAN",
            "year": "INTEGER",
            "month": "INTEGER",
            "day": "INTEGER",
        },
    },
    "stg_users": {
        "bucket": "silver",
        "prefix": "users_clean/",
        "columns": {
            "id": "INTEGER",
            "current_age": "INTEGER",
            "retirement_age": "INTEGER",
            "birth_year": "INTEGER",
            "birth_month": "INTEGER",
            "gender": "VARCHAR(20)",
            "address": "VARCHAR(255)",
            "latitude": "VARCHAR(50)",
            "longitude": "VARCHAR(50)",
            "per_capita_income": "DECIMAL(12,2)",
            "yearly_income": "DECIMAL(12,2)",
            "total_debt": "DECIMAL(12,2)",
            "credit_score": "INTEGER",
            "num_credit_cards": "INTEGER",
            "_cdc_timestamp": "BIGINT",
        },
    },
    "stg_cards": {
        "bucket": "silver",
        "prefix": "cards_clean/",
        "columns": {
            "id": "INTEGER",
            "client_id": "INTEGER",
            "card_brand": "VARCHAR(50)",
            "card_type": "VARCHAR(50)",
            "has_chip": "VARCHAR(10)",
            "num_cards_issued": "INTEGER",
            "credit_limit": "DECIMAL(12,2)",
            "acct_open_date": "VARCHAR(50)",
            "year_pin_last_changed": "INTEGER",
            "card_number_hash": "VARCHAR(256)",
            "card_number_masked": "VARCHAR(50)",
            "_cdc_timestamp": "BIGINT",
        },
    },
    "stg_mcc": {
        "bucket": "silver",
        "prefix": "mcc_clean/",
        "columns": {
            "mcc_id": "INTEGER",
            "description": "VARCHAR(255)",
        },
    },
}


def get_s3_client():
    """Tạo S3 client kết nối MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def get_pg_conn():
    """Tạo kết nối PostgreSQL"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )


def list_parquet_files(s3_client, bucket, prefix):
    """Liệt kê tất cả file .parquet trong bucket/prefix"""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])
    return files


def partition_values(key):
    """Lấy giá trị partition kiểu Hive từ đường dẫn, vd .../year=2022/month=1/day=5/part-0.parquet"""
    values = {}
    for part in key.split("/")[:-1]:
        if "=" in part:
            name, value = part.split("=", 1)
            values[name] = value
    return values


def load_table(s3_client, conn, table_name, config):
    """
    Nạp 1 bảng: đọc Parquet từ MinIO -> TRUNCATE + COPY vào PostgreSQL staging.
    Toàn bộ TRUNCATE + COPY nằm trong 1 transaction: nếu bất kỳ file nào lỗi thì
    rollback, staging giữ nguyên dữ liệu cũ thay vì bị nạp dở.
    """
    bucket = config["bucket"]
    prefix = config["prefix"]
    columns = config["columns"]
    table = f"{STAGING_SCHEMA}.{table_name}"
    column_defs = ",\n                ".join(f"{c} {t}" for c, t in columns.items())

    print(f"\n{'='*60}")
    print(f"[{table_name}] Bắt đầu nạp từ s3://{bucket}/{prefix}")

    # Liệt kê file Parquet trước khi đụng vào staging
    parquet_files = list_parquet_files(s3_client, bucket, prefix)

    # Tạo schema + bảng nếu chưa có, bổ sung cột mới cho bảng cũ
    # (commit ở cuối hàm, cùng transaction với COPY)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n                {column_defs}\n            )")
        for c, t in columns.items():
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {c} {t}")
        cur.execute(f"TRUNCATE TABLE {table}")

    if not parquet_files:
        print(f"  ⚠ Không tìm thấy file Parquet nào tại s3://{bucket}/{prefix}")
        conn.commit()
        return 0

    print(f"  → Tìm thấy {len(parquet_files)} file Parquet")

    total_rows = 0
    for pf in parquet_files:
        try:
            # Tải file Parquet vào bộ nhớ
            response = s3_client.get_object(Bucket=bucket, Key=pf)
            parquet_bytes = response["Body"].read()

            # Đọc bằng pyarrow
            arrow_table = pq.read_table(io.BytesIO(parquet_bytes))
            df = arrow_table.to_pandas()

            if df.empty:
                continue

            # Bổ sung cột partition lấy từ đường dẫn (không có trong nội dung file)
            for name, value in partition_values(pf).items():
                if name in columns and name not in df.columns:
                    df[name] = value

            # Lọc chỉ lấy các cột cần thiết (nếu có trong file)
            available_cols = [c for c in columns if c in df.columns]
            df = df[available_cols]

            # Ghi vào CSV buffer rồi COPY vào PostgreSQL (nhanh nhất)
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="\\N",
                      quoting=csv.QUOTE_MINIMAL)
            buffer.seek(0)

            with conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY {table} ({','.join(available_cols)}) "
                    f"FROM STDIN WITH CSV NULL '\\N'",
                    buffer,
                )
            total_rows += len(df)
        except Exception as e:
            raise RuntimeError(f"Lỗi nạp file {pf}: {e}") from e

    conn.commit()
    print(f"  ✓ Đã nạp {total_rows:,} bản ghi vào {table}")
    return total_rows


def main():
    print("=" * 60)
    print("BẮT ĐẦU NẠP DỮ LIỆU SILVER (MinIO Parquet) → PostgreSQL STAGING")
    print("=" * 60)

    s3_client = get_s3_client()
    conn = get_pg_conn()

    grand_total = 0
    failed_tables = []
    for table_name, config in TABLES_CONFIG.items():
        try:
            rows = load_table(s3_client, conn, table_name, config)
            grand_total += rows
        except Exception as e:
            print(f"  ✗ Lỗi nạp {table_name} (đã rollback, staging giữ dữ liệu cũ): {e}")
            conn.rollback()
            failed_tables.append(table_name)

    conn.close()
    print(f"\n{'='*60}")
    if failed_tables:
        # Exit code != 0 để task Airflow fail và dbt không chạy trên staging thiếu dữ liệu
        print(f"THẤT BẠI: Không nạp được {', '.join(failed_tables)}")
        print("=" * 60)
        sys.exit(1)
    print(f"HOÀN THÀNH: Tổng cộng {grand_total:,} bản ghi đã nạp vào schema '{STAGING_SCHEMA}'")
    print("=" * 60)


if __name__ == "__main__":
    main()
