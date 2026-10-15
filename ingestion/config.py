import os
from pathlib import Path
from dotenv import load_dotenv

# Nạp .env từ root project (2 cấp trên ingestion/)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE  = BASE_DIR / ".env"
load_dotenv(ENV_FILE if ENV_FILE.exists() else None)

# ==========================================
# CẤU HÌNH DATABASE (SQL SERVER)
# ==========================================
DB_SERVER     = os.environ["DB_SERVER"]
DB_PORT       = os.environ["DB_PORT"]
DB_NAME       = os.environ["DB_NAME"]
DB_SCHEMA     = os.environ["DB_SCHEMA"]
DB_USER       = os.environ["DB_USER"]
DB_PASSWORD   = os.environ["MSSQL_SA_PASSWORD"]
DB_DRIVER     = os.environ["DB_DRIVER"]
DB_TRUST_CERT = os.environ["DB_TRUST_CERT"]

# ==========================================
# CẤU HÌNH INGESTION / REPLAY
# ==========================================

# Đường dẫn file nguồn — khai báo trong .env: SOURCE_FILE_PATH
# Nếu là đường dẫn tương đối thì resolve so với root project
_raw_source = os.environ["SOURCE_FILE_PATH"]
_source_path = Path(_raw_source)
SOURCE_FILE_PATH = str(
    _source_path if _source_path.is_absolute() else BASE_DIR / _source_path
)
CSV_FILE_PATH = SOURCE_FILE_PATH  # alias tương thích ngược

# Đường dẫn file checkpoint — khai báo trong .env: CHECKPOINT_FILE_PATH
_raw_ckpt = os.environ["CHECKPOINT_FILE_PATH"]
_ckpt_path = Path(_raw_ckpt)
CHECKPOINT_FILE_PATH = str(
    _ckpt_path if _ckpt_path.is_absolute() else BASE_DIR / _ckpt_path
)

# Dead Letter Queue cho record bị SQL Server từ chối (vi phạm FK...) — khai báo trong .env: REJECTED_FILE_PATH
_raw_rejected = os.environ.get("REJECTED_FILE_PATH", "ingestion/dlq/rejected_transactions.jsonl")
_rejected_path = Path(_raw_rejected)
REJECTED_FILE_PATH = str(
    _rejected_path if _rejected_path.is_absolute() else BASE_DIR / _rejected_path
)

# Cấu hình Batch & Thời gian nghỉ
BATCH_SIZE             = int(os.environ["BATCH_SIZE"])     # record/batch
BATCH_INTERVAL_SECONDS = float(os.environ["BATCH_INTERVAL"])  # giây giữa batch
MAX_RETRIES            = int(os.environ["MAX_RETRIES"])    # retry khi mất kết nối
RETRY_DELAY_SECONDS    = float(os.environ["RETRY_DELAY"])  # giây chờ trước retry
