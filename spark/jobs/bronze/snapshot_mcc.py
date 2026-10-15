import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

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

# ── Đọc các biến môi trường ──
minio_user     = os.environ.get("MINIO_ROOT_USER")
minio_password = os.environ.get("MINIO_ROOT_PASSWORD")
db_server      = os.environ.get("DB_SERVER", "sqlserver")
# Nếu chạy trong Docker container mà DB_SERVER trong .env là localhost, tự chuyển sang sqlserver
if (db_server == "localhost" or db_server == "127.0.0.1") and Path("/opt/spark").exists():
    db_server = "sqlserver"
db_port        = os.environ.get("DB_PORT", "1433")
db_name        = os.environ.get("DB_NAME", "banking_db")
db_user        = os.environ.get("DB_USER", "sa")
db_password    = os.environ.get("MSSQL_SA_PASSWORD")

if not minio_user or not minio_password:
    raise ValueError("Lỗi: Thiếu biến môi trường MINIO_ROOT_USER hoặc MINIO_ROOT_PASSWORD trong .env!")

if not db_password:
    raise ValueError("Lỗi: Thiếu biến môi trường MSSQL_SA_PASSWORD trong .env!")

spark = (
    SparkSession.builder
    .appName("Bronze-MCC-Snapshot")
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

# Đọc trực tiếp snapshot từ SQL Server JDBC
jdbc_url = f"jdbc:sqlserver://{db_server}:{db_port};databaseName={db_name};encrypt=false;trustServerCertificate=true"
df_mcc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.mcc_codes")
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
    .withColumn("_snapshot_at", current_timestamp())
)

# Ghi đè vào Bronze layer
df_mcc.write.mode("overwrite").parquet("s3a://bronze/mcc_raw")
print("✓ Đã hoàn tất snapshot mcc_raw vào MinIO Bronze.")
