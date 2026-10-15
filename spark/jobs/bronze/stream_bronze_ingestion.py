"""
stream_bronze_ingestion.py
Job Spark Structured Streaming nạp dữ liệu CDC từ Kafka vào MinIO Bronze Layer:
- transactions_raw: partition theo year/month/day
- users_raw: unpartitioned
- cards_raw: unpartitioned
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, 
    year, month, dayofmonth, to_timestamp
)
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, LongType, BooleanType
)
import functools
print = functools.partial(print, flush=True)

# spark/jobs/ vào sys.path để import module dùng chung common/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.streaming_metrics import PushgatewayListener  # noqa: E402

# ── Hàm tự nạp .env mà không phụ thuộc thư viện bên ngoài (pure Python) ──
def load_env():
    candidates = [
        Path("/opt/spark/.env"),
        Path("/opt/spark/apps/.env"),
        Path(__file__).resolve().parent.parent.parent.parent / ".env",
        Path(".env")
    ]
    for env_path in candidates:
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip().strip("'\""))
            break

load_env()

# ── 1. Cấu hình Spark Session kết nối MinIO (S3A) & Kafka ──
def create_spark_session():
    minio_user = os.environ.get("MINIO_ROOT_USER")
    minio_password = os.environ.get("MINIO_ROOT_PASSWORD")

    if not minio_user or not minio_password:
        raise ValueError("Lỗi: Thiếu biến môi trường MINIO_ROOT_USER hoặc MINIO_ROOT_PASSWORD trong .env!")

    return (
        SparkSession.builder
        .appName("Bronze-Streaming-Ingestion")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800")
        .getOrCreate()
    )

# ── 2. Định nghĩa Schema cho Payload Debezium của từng bảng ──
# Debezium bọc dữ liệu trong {"payload": {"after": {...}, "op": "c", ...}}

schema_transactions = StructType([
    StructField("id", IntegerType(), False),
    StructField("date", LongType(), True),         # Timestamp epoch ms từ Debezium
    StructField("client_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("errors", StringType(), True)
])

schema_users = StructType([
    StructField("id", IntegerType(), False),
    StructField("current_age", IntegerType(), True),
    StructField("retirement_age", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("per_capita_income", DecimalType(12, 2), True),
    StructField("yearly_income", DecimalType(12, 2), True),
    StructField("total_debt", DecimalType(12, 2), True),
    StructField("credit_score", IntegerType(), True),
    StructField("num_credit_cards", IntegerType(), True)
])

schema_cards = StructType([
    StructField("id", IntegerType(), False),
    StructField("client_id", IntegerType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", IntegerType(), True),
    StructField("credit_limit", DecimalType(12, 2), True),
    StructField("acct_open_date", StringType(), True),
    StructField("year_pin_last_changed", IntegerType(), True)
])

def create_debezium_envelope(after_schema):
    return StructType([
        StructField("payload", StructType([
            StructField("after", after_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True)
        ]), True)
    ])

# ── 3. Hàm xử lý stream cho từng bảng ──
def start_stream(spark, topic_name, target_table, schema, partition_cols=None):
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    envelope_schema = create_debezium_envelope(schema)

    # Đọc Stream từ Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Bóc tách JSON Debezium & bổ sung Technical Metadata
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), envelope_schema).alias("data"))
        .select(
            col("data.payload.after.*"),
            col("data.payload.op").alias("_cdc_operation"),
            col("data.payload.ts_ms").alias("_cdc_timestamp"),
            current_timestamp().alias("_ingested_at")
        )
        .filter(col("id").isNotNull()) # Loại bỏ bản ghi delete tombstone
    )

    # Riêng bảng transactions: Trích xuất date sang year, month, day để partition
    if target_table == "transactions_raw":
        # date từ Debezium là epoch ms
        parsed_df = (
            parsed_df
            .withColumn("tx_timestamp", to_timestamp(col("date") / 1000))
            .withColumn("year", year(col("tx_timestamp")))
            .withColumn("month", month(col("tx_timestamp")))
            .withColumn("day", dayofmonth(col("tx_timestamp")))
        )

    # Ghi vào MinIO Bronze Bucket ở định dạng Parquet
    sink_path = f"s3a://bronze/{target_table}"
    checkpoint_path = f"s3a://bronze/_checkpoints/{target_table}"

    writer = (
        parsed_df.writeStream
        .queryName(f"bronze_{target_table}")  # Tên cố định để định danh metrics trên Prometheus
        .format("parquet")
        .option("path", sink_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    return writer.start()

# ── 4. Main Entrypoint: Chạy đồng thời cả 3 Streams ──
if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    # Đẩy throughput / Kafka lag / heartbeat của từng stream lên Pushgateway (mục 3.7)
    spark.streams.addListener(PushgatewayListener("spark_bronze_streaming"))

    # Khởi động 3 Streams
    q_trans = start_stream(
        spark, 
        topic_name="banking.banking_db.dbo.transactions",
        target_table="transactions_raw",
        schema=schema_transactions,
        partition_cols=["year", "month", "day"]
    )

    q_users = start_stream(
        spark,
        topic_name="banking.banking_db.dbo.users",
        target_table="users_raw",
        schema=schema_users
    )

    q_cards = start_stream(
        spark,
        topic_name="banking.banking_db.dbo.cards",
        target_table="cards_raw",
        schema=schema_cards
    )

    # Chờ các streams cùng chạy liên tục
    spark.streams.awaitAnyTermination()
