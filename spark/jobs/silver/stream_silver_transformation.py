"""
stream_silver_transformation.py
Job Spark Structured Streaming & Batch biến đổi dữ liệu từ MinIO Bronze Layer sang Silver Layer:
- Làm sạch, chuẩn hóa kiểu dữ liệu, khử trùng lặp (deduplication)
- Mã hóa dữ liệu định danh (PII): Hash SHA-256 có salt cho card_number, CVV không được lưu (mục 3.1.2 & 3.4)
- Row-level Data Quality & Dead Letter Queue (DLQ) theo mục 3.6:
  + Bản ghi hợp lệ (Completeness, Validity, Timeliness) -> s3a://silver/transactions_clean/
  + Bản ghi lỗi -> s3a://silver/dlq/transactions_rejected/ kèm metadata nguyên nhân
- Mã hóa at-rest: bucket bronze/silver được bật SSE-S3 mặc định bởi service minio-init (docker-compose)
"""

import os
import sys
import argparse
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, trim, substring, concat, sha2, row_number,
    current_timestamp, to_timestamp, year, month, dayofmonth, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, LongType, TimestampType, BooleanType
)
import functools
print = functools.partial(print, flush=True)

# spark/jobs/ vào sys.path để import module dùng chung common/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.streaming_metrics import PushgatewayListener, push_metrics  # noqa: E402


# ── 1. Tự động nạp cấu hình từ .env (Pure Python) ──
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

PII_HASH_SALT = os.environ.get("PII_HASH_SALT")
if not PII_HASH_SALT:
    raise ValueError("Lỗi: Thiếu biến môi trường PII_HASH_SALT trong .env (salt để hash card_number)!")


# ── 2. Khởi tạo Spark Session (MinIO S3A; SSE-S3 do bucket tự áp dụng) ──
def create_spark_session(app_name="Silver-Transformation-Job"):
    minio_user = os.environ.get("MINIO_ROOT_USER")
    minio_password = os.environ.get("MINIO_ROOT_PASSWORD")

    if not minio_user or not minio_password:
        raise ValueError("Lỗi: Thiếu biến môi trường MINIO_ROOT_USER hoặc MINIO_ROOT_PASSWORD trong .env!")

    return (
        SparkSession.builder
        .appName(app_name)
        # Mặc định 200 partition cho mỗi shuffle: với micro-batch vài nghìn dòng trên 2 core,
        # mỗi batch mất ~10 phút chỉ để lên lịch task rỗng
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
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


# ── 3. Helper kiểm tra / tạo thư mục S3A ──
def ensure_s3a_path(spark, s3a_path):
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(s3a_path)
        fs = path.getFileSystem(conf)
        if not fs.exists(path):
            fs.mkdirs(path)
            print(f"✓ Đã tạo bucket/path: {s3a_path}")
        return True
    except Exception as e:
        print(f"! Thông tin S3A path {s3a_path}: {e}")
        return False


# ── 4. Schema Bronze để đọc Stream Parquet ──
schema_bronze_transactions = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", LongType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("errors", StringType(), True),
    StructField("_cdc_operation", StringType(), True),
    StructField("_cdc_timestamp", LongType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("tx_timestamp", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True)
])

schema_bronze_users = StructType([
    StructField("id", IntegerType(), True),
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
    StructField("num_credit_cards", IntegerType(), True),
    StructField("_cdc_operation", StringType(), True),
    StructField("_cdc_timestamp", LongType(), True),
    StructField("_ingested_at", TimestampType(), True)
])

schema_bronze_cards = StructType([
    StructField("id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", IntegerType(), True),
    StructField("credit_limit", DecimalType(12, 2), True),
    StructField("acct_open_date", StringType(), True),
    StructField("year_pin_last_changed", IntegerType(), True),
    StructField("_cdc_operation", StringType(), True),
    StructField("_cdc_timestamp", LongType(), True),
    StructField("_ingested_at", TimestampType(), True)
])


SILVER_TRANSACTIONS_PATH = "s3a://silver/transactions_clean"
SILVER_DLQ_PATH = "s3a://silver/dlq/transactions_rejected"

# Tổng số bản ghi hợp lệ / bị đẩy vào DLQ kể từ khi driver khởi động (Failed Records - mục 3.7)
_silver_totals = {"valid": 0, "rejected": 0}


# ── 5. Helpers khử trùng lặp ──
def keep_latest_per_id(df):
    """Giữ bản ghi CDC mới nhất cho mỗi id (theo _cdc_timestamp)."""
    w = Window.partitionBy("id").orderBy(col("_cdc_timestamp").desc_nulls_last())
    return (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def exclude_existing_ids(spark, df, target_path, partition_cols=None):
    """
    Loại các id đã có ở đích để append idempotent: Debezium/foreachBatch là
    at-least-once nên cùng một giao dịch có thể đến nhiều lần.
    partition_cols: chỉ đọc các partition có trong batch (id luôn nằm đúng partition ngày của nó).
    """
    try:
        existing = spark.read.parquet(target_path)
    except AnalysisException:
        return df  # Đích chưa có dữ liệu
    if partition_cols:
        batch_partitions = df.select(*partition_cols).distinct()
        existing = existing.join(batch_partitions, on=partition_cols, how="left_semi")
    return df.join(existing.select("id").distinct(), on="id", how="left_anti")


# ── 6. Logic xử lý Micro-Batch cho Transactions & Tách DLQ (Mục 3.6) ──
def process_transactions_batch(df, batch_id, rebuild=False):
    """
    rebuild=False (streaming): append, bỏ qua id đã có ở Silver / DLQ.
    rebuild=True  (batch):     dựng lại toàn bộ Silver từ Bronze bằng overwrite.
    """
    if df.rdd.isEmpty():
        return

    spark = df.sparkSession
    write_mode = "overwrite" if rebuild else "append"

    print(f"\n[Batch {batch_id}] Đang xử lý {df.count()} bản ghi transactions...")

    # Khử trùng lặp bản ghi theo ID (giữ bản CDC mới nhất)
    batch_df = keep_latest_per_id(df)

    # Chuẩn hóa tx_timestamp nếu null từ epoch ms date
    batch_df = batch_df.withColumn(
        "tx_timestamp",
        coalesce(col("tx_timestamp"), to_timestamp(col("date") / 1000))
    )

    # Đảm bảo có partition year, month, day
    batch_df = (
        batch_df
        .withColumn("year", coalesce(col("year"), year(col("tx_timestamp"))))
        .withColumn("month", coalesce(col("month"), month(col("tx_timestamp"))))
        .withColumn("day", coalesce(col("day"), dayofmonth(col("tx_timestamp"))))
    ).persist()  # Dùng lại cho cả nhánh hợp lệ lẫn DLQ, tránh đọc lại nguồn nhiều lần

    # ── ĐIỀU KIỆN DATA QUALITY (MỤC 3.6) ──
    # 1. Completeness: Bắt buộc không NULL
    is_complete = (
        col("id").isNotNull() &
        col("client_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        col("tx_timestamp").isNotNull()
    )

    # 2. Validity: amount khác 0 (amount < 0 là giao dịch hoàn tiền, hợp lệ và được gắn cờ is_refund)
    is_valid_amount = (col("amount") != 0)

    # 3. Timeliness: timestamp không lớn hơn thời điểm hiện tại
    is_timely = (col("tx_timestamp") <= current_timestamp())

    # Tổng hợp điều kiện hợp lệ
    is_valid = is_complete & is_valid_amount & is_timely

    # ── TÁCH BẢN GHI HỢP LỆ (SILVER TRANSACTIONS CLEAN) ──
    valid_df = (
        batch_df
        .filter(is_valid)
        .withColumn(
            "is_error", 
            when(
                col("errors").isNotNull() & 
                (trim(col("errors")) != "") & 
                (col("errors") != "null"), 
                True
            ).otherwise(False)
        )
        .withColumn("is_refund", col("amount") < 0)
        .withColumn("_transformed_at", current_timestamp())
    )
    if not rebuild:
        valid_df = exclude_existing_ids(spark, valid_df, SILVER_TRANSACTIONS_PATH, ["year", "month", "day"])
    valid_df = valid_df.persist()

    valid_count = valid_df.count()
    if valid_count > 0 or rebuild:
        valid_df.write \
            .mode(write_mode) \
            .format("parquet") \
            .partitionBy("year", "month", "day") \
            .save(SILVER_TRANSACTIONS_PATH)
        print(f"  ✓ Đã ghi ({write_mode}) {valid_count} bản ghi hợp lệ vào {SILVER_TRANSACTIONS_PATH}")
    valid_df.unpersist()

    # ── TÁCH BẢN GHI VI PHẠM VÀO DEAD LETTER QUEUE (DLQ) ──
    invalid_df = (
        batch_df
        .filter(~is_valid)
        .withColumn(
            "dlq_error_code",
            when(~is_complete, lit("ERR_COMPLETENESS"))
            .when(~is_valid_amount, lit("ERR_INVALID_AMOUNT"))
            .when(~is_timely, lit("ERR_FUTURE_TIMESTAMP"))
            .otherwise(lit("ERR_UNKNOWN_VALIDATION"))
        )
        .withColumn(
            "dlq_error_reason",
            when(col("id").isNull(), lit("Thiếu trường bắt buộc: id IS NULL"))
            .when(col("client_id").isNull(), lit("Thiếu trường bắt buộc: client_id IS NULL"))
            .when(col("card_id").isNull(), lit("Thiếu trường bắt buộc: card_id IS NULL"))
            .when(col("amount").isNull(), lit("Thiếu trường bắt buộc: amount IS NULL"))
            .when(col("tx_timestamp").isNull(), lit("Thiếu trường bắt buộc: timestamp IS NULL"))
            .when(col("amount") == 0, lit("Giá trị amount không hợp lệ: amount = 0"))
            .when(col("tx_timestamp") > current_timestamp(), lit("Thời gian giao dịch vượt quá hiện tại (Timeliness)"))
            .otherwise(lit("Vi phạm quy tắc chất lượng dữ liệu Silver"))
        )
        .withColumn("_rejected_at", current_timestamp())
    )
    if not rebuild:
        invalid_df = exclude_existing_ids(spark, invalid_df, SILVER_DLQ_PATH)
    invalid_df = invalid_df.persist()

    invalid_count = invalid_df.count()
    if invalid_count > 0 or rebuild:
        invalid_df.write \
            .mode(write_mode) \
            .format("parquet") \
            .save(SILVER_DLQ_PATH)
        print(f"  ! Đã cách ly ({write_mode}) {invalid_count} bản ghi vi phạm vào Dead Letter Queue ({SILVER_DLQ_PATH})")
    invalid_df.unpersist()
    batch_df.unpersist()

    if not rebuild:
        _silver_totals["valid"] += valid_count
        _silver_totals["rejected"] += invalid_count
        push_metrics("spark_silver_streaming", {"query": "silver_transactions"}, {
            "silver_transactions_valid_total": _silver_totals["valid"],
            "silver_transactions_dlq_total": _silver_totals["rejected"],
        })


# ── 7. Logic xử lý Micro-Batch cho Cards (Masking PII) ──
def process_cards_batch(df, batch_id, rebuild=False):
    if df.rdd.isEmpty():
        return

    print(f"\n[Batch {batch_id}] Đang xử lý {df.count()} bản ghi cards...")

    # Khử trùng lặp (giữ bản CDC mới nhất) và làm sạch
    clean_cards = (
        keep_latest_per_id(df.filter(col("id").isNotNull()))
        # Tokenization / Masking theo mục 3.1.2 & 3.4
        # Keyed hash (salt bí mật): SHA-256 thuần của số thẻ 16 chữ số có thể dò ngược bằng brute-force
        .withColumn("card_number_hash", sha2(concat(lit(PII_HASH_SALT), col("card_number")), 256))
        .withColumn("card_number_masked", concat(lit("****-****-****-"), substring(col("card_number"), -4, 4)))
        .drop("card_number")  # Tuyệt đối không lưu raw card_number ở tầng Silver
        .drop("cvv")  # CVV bị loại ngay tại Debezium (column.exclude.list); drop phòng dữ liệu cũ
        .withColumn("_transformed_at", current_timestamp())
    )

    clean_cards.write \
        .mode("overwrite" if rebuild else "append") \
        .format("parquet") \
        .save("s3a://silver/cards_clean")
    print(f"  ✓ Đã ghi sạch & mask PII vào s3a://silver/cards_clean")


# ── 8. Logic xử lý Micro-Batch cho Users ──
def process_users_batch(df, batch_id, rebuild=False):
    if df.rdd.isEmpty():
        return

    print(f"\n[Batch {batch_id}] Đang xử lý {df.count()} bản ghi users...")

    clean_users = (
        keep_latest_per_id(df.filter(col("id").isNotNull()))
        .withColumn("_transformed_at", current_timestamp())
    )

    clean_users.write \
        .mode("overwrite" if rebuild else "append") \
        .format("parquet") \
        .save("s3a://silver/users_clean")
    print(f"  ✓ Đã ghi vào s3a://silver/users_clean")


# ── 9. Batch Snapshot cho MCC Codes (Dimension tĩnh) ──
def sync_mcc_silver(spark, strict=False):
    """strict=True (mode mcc, chạy từ DAG snapshot MCC): lỗi thì job fail để Airflow thấy."""
    try:
        print("\n[MCC Snapshot] Đồng bộ mcc_codes từ Bronze sang Silver...")
        mcc_df = spark.read.parquet("s3a://bronze/mcc_raw")
        clean_mcc = (
            mcc_df.filter(col("mcc_id").isNotNull())
            .dropDuplicates(["mcc_id"])
            .withColumn("_transformed_at", current_timestamp())
        )
        clean_mcc.write.mode("overwrite").parquet("s3a://silver/mcc_clean")
        print(f"  ✓ Đã đồng bộ {clean_mcc.count()} dòng mcc_clean vào s3a://silver/mcc_clean")
    except Exception as e:
        if strict:
            raise
        print(f"  ! Chưa tìm thấy s3a://bronze/mcc_raw hoặc bỏ qua: {e}")


# ── 10. Chế độ thực thi Batch (cho Airflow và kiểm thử) ──
# Dựng lại toàn bộ Silver từ Bronze bằng overwrite (idempotent, xóa luôn bản ghi trùng cũ).
# KHÔNG chạy song song với run_streaming: hai bên cùng ghi vào một thư mục Silver.
# Chỉ bỏ qua khi Bronze chưa có dữ liệu; lỗi khác để job fail (exit code != 0) cho Airflow thấy.
def run_batch_sync(spark):
    print("=== BẮT ĐẦU CHẾ ĐỘ BATCH REBUILD (BRONZE -> SILVER, OVERWRITE) ===")

    # 1. Transactions
    try:
        df_trans = spark.read.schema(schema_bronze_transactions).parquet("s3a://bronze/transactions_raw")
    except AnalysisException as e:
        print(f"! Chưa có s3a://bronze/transactions_raw, bỏ qua: {e}")
    else:
        process_transactions_batch(df_trans, "batch_job", rebuild=True)

    # 2. Users
    try:
        df_users = spark.read.schema(schema_bronze_users).parquet("s3a://bronze/users_raw")
    except AnalysisException as e:
        print(f"! Chưa có s3a://bronze/users_raw, bỏ qua: {e}")
    else:
        process_users_batch(df_users, "batch_job", rebuild=True)

    # 3. Cards
    try:
        df_cards = spark.read.schema(schema_bronze_cards).parquet("s3a://bronze/cards_raw")
    except AnalysisException as e:
        print(f"! Chưa có s3a://bronze/cards_raw, bỏ qua: {e}")
    else:
        process_cards_batch(df_cards, "batch_job", rebuild=True)

    # 4. MCC
    sync_mcc_silver(spark)
    print("=== HOÀN TẤT BATCH TRANSFORM ===")


# ── 11. Chế độ thực thi Streaming liên tục (Daemon) ──
def run_streaming(spark):
    print("=== BẮT ĐẦU CHẾ ĐỘ STREAMING TRANSFORM (BRONZE -> SILVER) ===")
    # Đẩy throughput / heartbeat của từng stream lên Pushgateway (mục 3.7)
    spark.streams.addListener(PushgatewayListener("spark_silver_streaming"))
    
    # Đồng bộ MCC trước
    sync_mcc_silver(spark)

    # 1. Stream Transactions
    stream_trans = (
        spark.readStream
        .schema(schema_bronze_transactions)
        .option("maxFilesPerTrigger", 50)
        .parquet("s3a://bronze/transactions_raw")
        .writeStream
        .queryName("silver_transactions")
        .foreachBatch(process_transactions_batch)
        .option("checkpointLocation", "s3a://silver/_checkpoints/transactions_clean")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # 2. Stream Cards
    stream_cards = (
        spark.readStream
        .schema(schema_bronze_cards)
        .option("maxFilesPerTrigger", 20)
        .parquet("s3a://bronze/cards_raw")
        .writeStream
        .queryName("silver_cards")
        .foreachBatch(process_cards_batch)
        .option("checkpointLocation", "s3a://silver/_checkpoints/cards_clean")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 3. Stream Users
    stream_users = (
        spark.readStream
        .schema(schema_bronze_users)
        .option("maxFilesPerTrigger", 20)
        .parquet("s3a://bronze/users_raw")
        .writeStream
        .queryName("silver_users")
        .foreachBatch(process_users_batch)
        .option("checkpointLocation", "s3a://silver/_checkpoints/users_clean")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("✓ Cả 3 stream Silver đang lắng nghe Bronze...")
    spark.streams.awaitAnyTermination()


# ── MAIN ENTRYPOINT ──
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Silver Transformation & DLQ Job")
    parser.add_argument(
        "--mode", 
        choices=["streaming", "batch", "mcc"],
        default="batch",
        help="Chế độ chạy: 'streaming' (liên tục), 'batch' (dựng lại Silver từ Bronze) "
             "hoặc 'mcc' (chỉ đồng bộ mcc_codes Bronze -> Silver, dùng sau snapshot MCC)"
    )
    args = parser.parse_args()

    spark = create_spark_session(f"Silver-Transformation-{args.mode.upper()}")
    spark.sparkContext.setLogLevel("WARN")

    # Đảm bảo đường dẫn đích tồn tại
    ensure_s3a_path(spark, "s3a://silver/")

    if args.mode == "streaming":
        run_streaming(spark)
    elif args.mode == "mcc":
        # Streaming chỉ đồng bộ MCC lúc khởi động; snapshot MCC hàng tuần cần tự đẩy sang Silver
        sync_mcc_silver(spark, strict=True)
    else:
        run_batch_sync(spark)
