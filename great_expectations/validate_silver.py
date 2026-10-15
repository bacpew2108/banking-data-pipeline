"""
validate_silver.py
Script thực thi kiểm tra chất lượng dữ liệu tầng Silver bằng Great Expectations (Mục 3.6).
- Đọc dữ liệu từ MinIO S3 (bucket 'silver', thư mục 'transactions_clean')
- Kiểm tra 5 tiêu chí: Completeness, Validity, Consistency, Uniqueness, Timeliness
- Rà soát thống kê Dead Letter Queue (s3a://silver/dlq/transactions_rejected)
- Sinh báo cáo Data Docs trực quan dạng HTML
- Trả về exit code 0 nếu pass (để Airflow trigger dbt) hoặc exit code 1 nếu fail (chặn pipeline)
"""

import os
import sys
import time
import argparse
import urllib.request
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

# ── 1. Nạp biến môi trường từ .env ──
def load_env():
    candidates = [
        Path("/opt/spark/.env"),
        Path("/opt/airflow/.env"),
        Path(__file__).resolve().parent.parent / ".env",
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


# ── 2. Đọc Parquet từ MinIO bằng PyArrow S3FileSystem ──
def read_silver_transactions(endpoint=None):
    import pyarrow.fs as pafs
    import pyarrow.dataset as pads
    import pandas as pd

    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]
    
    # Xác định endpoint
    if not endpoint:
        # Nếu chạy trong mạng Docker dùng minio:9000, bên ngoài dùng localhost:9000
        in_docker = Path("/.dockerenv").exists()
        endpoint = "minio:9000" if in_docker else "localhost:9000"

    print(f"[*] Kết nối MinIO tại {endpoint} (user: {minio_user})...")

    s3_fs = pafs.S3FileSystem(
        access_key=minio_user,
        secret_key=minio_password,
        endpoint_override=endpoint,
        scheme="http"
    )

    # Đọc transactions_clean
    # Chỉ coi là "chưa có dữ liệu" khi đường dẫn không tồn tại; lỗi kết nối / quyền
    # truy cập (OSError khác) được raise để Quality Gate FAIL thay vì cho qua.
    target_path = "silver/transactions_clean"
    try:
        dataset = pads.dataset(target_path, filesystem=s3_fs, format="parquet", partitioning="hive")
    except FileNotFoundError as e:
        print(f"! Chưa có dữ liệu tại {target_path}: {e}")
        return None, s3_fs
    table = dataset.to_table()
    df = table.to_pandas()
    print(f"✓ Đã đọc {len(df)} bản ghi từ '{target_path}'")
    return df, s3_fs


# ── 3. Thống kê Dead Letter Queue (DLQ) ──
def check_dlq_metrics(s3_fs):
    import pyarrow.dataset as pads
    dlq_path = "silver/dlq/transactions_rejected"
    try:
        dataset = pads.dataset(dlq_path, filesystem=s3_fs, format="parquet")
        table = dataset.to_table()
        dlq_df = table.to_pandas()
        count = len(dlq_df)
        print(f"\n[DLQ Monitoring] Dead Letter Queue đang lưu {count} bản ghi bị từ chối.")
        if count > 0 and "dlq_error_reason" in dlq_df.columns:
            print("  Top lý do vi phạm:")
            reasons = dlq_df["dlq_error_reason"].value_counts().head(5)
            for reason, cnt in reasons.items():
                print(f"    - [{cnt} bản ghi]: {reason}")
        return count
    except Exception:
        print("\n[DLQ Monitoring] Dead Letter Queue trống hoặc chưa phát sinh bản ghi lỗi.")
        return 0


# ── 4. Thực thi kiểm tra với Great Expectations ──
def validate_data(df):
    import great_expectations as gx

    gx_root = Path(__file__).resolve().parent
    context = gx.get_context(project_root_dir=str(gx_root))
    suite_name = "silver_transactions_suite"

    # Lấy suite
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        print(f"! Suite '{suite_name}' chưa tồn tại. Đang khởi tạo...")
        from build_suite import setup_suite
        suite = setup_suite()

    # Tạo Data Source & Asset cho Pandas DataFrame
    data_source_name = "silver_minio_datasource"
    try:
        data_source = context.data_sources.get(data_source_name)
    except Exception:
        data_source = context.data_sources.add_pandas(data_source_name)

    asset_name = "silver_transactions_asset"
    try:
        data_asset = data_source.get_asset(asset_name)
    except Exception:
        data_asset = data_source.add_dataframe_asset(name=asset_name)

    # Tạo Batch Definition
    batch_def_name = "transactions_full_batch"
    try:
        batch_def = data_asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = data_asset.add_batch_definition_whole_dataframe(batch_def_name)

    # Lấy Batch từ DataFrame
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Chạy Validation
    print(f"\n[*] Đang chạy Expectation Suite '{suite_name}'...")
    validation_result = batch.validate(suite)

    # In kết quả tóm tắt
    success = validation_result.success
    stats = validation_result.statistics
    
    print("\n" + "=" * 50)
    print("=== BÁO CÁO KIỂM TRA CHẤT LƯỢNG DỮ LIỆU (MỤC 3.6) ===")
    print(f"Trạng thái tổng thể: {'✓ PASS' if success else '✗ FAIL'}")
    print(f"Tổng số luật (Rules): {stats.get('evaluated_expectations', 0)}")
    print(f"Số luật đạt (Successful): {stats.get('successful_expectations', 0)}")
    print(f"Số luật vi phạm (Unsuccessful): {stats.get('unsuccessful_expectations', 0)}")
    print(f"Tỷ lệ thành công: {stats.get('success_percent', 0):.2f}%")
    print("=" * 50)

    # Hiển thị chi tiết các luật vi phạm nếu có
    if not success:
        print("\n! Danh sách các Expectation bị vi phạm:")
        for res in validation_result.results:
            if not res.success:
                exp_type = res.expectation_config.type
                kwargs = res.expectation_config.kwargs
                col = kwargs.get("column", "bảng")
                notes = res.expectation_config.notes or ""
                print(f"  - Cột: {col} | Loại lỗi: {exp_type} | {notes}")

    # Sinh và cập nhật Data Docs HTML
    try:
        context.build_data_docs()
        docs_path = gx_root / "uncommitted" / "data_docs" / "local_site" / "index.html"
        print(f"\n✓ Đã sinh báo cáo Data Docs HTML tại:\n  file://{docs_path}")
    except Exception as e:
        print(f"! Không thể sinh Data Docs: {e}")

    return success, stats.get("success_percent", 0)


# ── 5. Đẩy kết quả Quality Gate lên Prometheus Pushgateway (mục 3.7) ──
def push_quality_metrics(success, silver_records, dlq_records, success_percent):
    """Lỗi khi đẩy metrics chỉ cảnh báo, không làm thay đổi kết quả của gate."""
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    metrics = {
        "data_quality_gate_success": 1 if success else 0,
        "data_quality_success_percent": success_percent,
        "data_quality_silver_records": silver_records,
        "data_quality_dlq_records": dlq_records,
        "data_quality_last_run_timestamp_seconds": time.time(),
    }
    body = "".join(f"{name} {float(value)}\n" for name, value in metrics.items()).encode("utf-8")
    try:
        request = urllib.request.Request(
            f"{url.rstrip('/')}/metrics/job/great_expectations", data=body, method="PUT"
        )
        with urllib.request.urlopen(request, timeout=5):
            pass
        print(f"✓ Đã đẩy kết quả Quality Gate lên Pushgateway ({url})")
    except Exception as e:
        print(f"! Không đẩy được metrics lên Pushgateway ({url}): {e}")


# ── MAIN ENTRYPOINT ──
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runner kiểm tra Data Quality Silver với Great Expectations")
    parser.add_argument("--endpoint", help="MinIO endpoint (vd: localhost:9000 hoặc minio:9000)")
    parser.add_argument("--allow-empty", action="store_true", help="Không báo lỗi nếu dataset Silver rỗng")
    args = parser.parse_args()

    try:
        df, s3_fs = read_silver_transactions(endpoint=args.endpoint)
    except Exception as e:
        print(f"[ERROR] Không đọc được dữ liệu Silver (MinIO / quyền truy cập / file hỏng): {e}")
        push_quality_metrics(False, 0, 0, 0)
        sys.exit(1)

    # Giám sát DLQ
    dlq_count = check_dlq_metrics(s3_fs)

    if df is None or len(df) == 0:
        if args.allow_empty:
            print("[INFO] Dataset rỗng nhưng được bật cờ --allow-empty. Trả về thành công.")
            push_quality_metrics(True, 0, dlq_count, 100)
            sys.exit(0)
        else:
            print("[ERROR] Chưa có dữ liệu trong Silver để kiểm tra. Hãy chạy Spark Silver Ingestion trước.")
            push_quality_metrics(False, 0, dlq_count, 0)
            sys.exit(1)

    is_passed, success_percent = validate_data(df)
    push_quality_metrics(is_passed, len(df), dlq_count, success_percent)

    # Exit code: 0 = PASS, 1 = FAIL
    sys.exit(0 if is_passed else 1)
