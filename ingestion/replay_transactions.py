#!/usr/bin/env python3
"""
replay_transactions.py
-----------------------
Script ingestion: đọc file nguồn (CSV hoặc SQL), sắp xếp theo cột date tăng dần,
rồi insert vào bảng transactions theo batch qua pyodbc.

Sau mỗi batch thành công:
  - Ghi checkpoint_state.json để resume khi script bị dừng.

Sử dụng INSERT thường (không dùng BULK INSERT / TABLOCK) để Debezium CDC
bắt được từng thay đổi.

Cách dùng:
    python replay_transactions.py [--reset] [--dry-run] [--batch-size N] [--interval S]

Options:
    --reset         Xóa checkpoint, bắt đầu lại từ đầu
    --dry-run       Không thực sự insert, chỉ in ra record sẽ được insert
    --batch-size N  Ghi đè BATCH_SIZE (mặc định lấy từ config/env)
    --interval S    Ghi đè BATCH_INTERVAL_SECONDS (mặc định lấy từ config/env)
    --limit N       Chỉ insert N record rồi dừng (dùng để test)
"""

import argparse
import csv
import json
import logging
import os
import re
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Thêm thư mục gốc của ingestion vào sys.path để import config, checkpoint ──
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import pyodbc  # noqa: E402

from checkpoint import load_checkpoint, mark_completed, reset_checkpoint, save_checkpoint  # noqa: E402
from config import (  # noqa: E402
    BATCH_INTERVAL_SECONDS,
    BATCH_SIZE,
    CHECKPOINT_FILE_PATH,
    CSV_FILE_PATH,
    DB_DRIVER,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_SERVER,
    DB_TRUST_CERT,
    DB_USER,
    MAX_RETRIES,
    REJECTED_FILE_PATH,
    RETRY_DELAY_SECONDS,
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "replay_transactions.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("replay_transactions")

# ── SQL Insert statement ───────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO {schema}.transactions
    (id, [date], client_id, card_id, amount, use_chip,
     merchant_id, merchant_city, merchant_state, zip, mcc, errors)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip()

# ── Columns theo thứ tự trong CSV / SQL source ────────────────────────────────
COLUMNS = [
    "id", "date", "client_id", "card_id", "amount", "use_chip",
    "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors",
]

# ── Graceful shutdown flag ─────────────────────────────────────────────────────
_shutdown_requested = False


def _handle_signal(signum, frame):
    """Bắt SIGINT / SIGTERM để dừng sạch sau batch hiện tại."""
    global _shutdown_requested
    logger.warning("Nhận signal %s. Sẽ dừng sau batch hiện tại...", signum)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers: Kết nối DB
# ═══════════════════════════════════════════════════════════════════════════════

def build_connection_string() -> str:
    return (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"TrustServerCertificate={DB_TRUST_CERT};"
        f"Encrypt=yes;"
    )


def get_connection(retries: int = MAX_RETRIES) -> pyodbc.Connection:
    """
    Tạo kết nối pyodbc với retry logic.

    Args:
        retries: Số lần thử lại tối đa.

    Returns:
        pyodbc.Connection đã kết nối.

    Raises:
        SystemExit nếu không thể kết nối sau tất cả retries.
    """
    conn_str = build_connection_string()
    for attempt in range(1, retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            conn.autocommit = False
            logger.info(
                "Kết nối SQL Server thành công (attempt %d/%d).", attempt, retries
            )
            return conn
        except pyodbc.Error as exc:
            logger.warning(
                "Kết nối thất bại (attempt %d/%d): %s", attempt, retries, exc
            )
            if attempt < retries:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.critical("Không thể kết nối SQL Server sau %d lần. Dừng.", retries)
                sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers: Đọc dữ liệu nguồn
# ═══════════════════════════════════════════════════════════════════════════════

def _coerce_row(row: dict) -> tuple:
    """
    Chuyển đổi kiểu dữ liệu cho một record trước khi insert.
    Trả về tuple theo thứ tự COLUMNS.
    """
    def _int_or_none(v):
        if v is None or str(v).strip() in ("", "NULL", "None"):
            return None
        return int(str(v).strip())

    def _float_or_none(v):
        if v is None or str(v).strip() in ("", "NULL", "None"):
            return None
        return float(str(v).strip().replace("$", "").replace(",", ""))

    def _str_or_none(v):
        if v is None or str(v).strip() in ("", "NULL", "None"):
            return None
        return str(v).strip()

    return (
        _int_or_none(row.get("id")),          # id INT
        _str_or_none(row.get("date")),         # date DATETIME (string → SQL casts tự động)
        _int_or_none(row.get("client_id")),    # client_id INT
        _int_or_none(row.get("card_id")),      # card_id INT
        _float_or_none(row.get("amount")),     # amount DECIMAL
        _str_or_none(row.get("use_chip")),     # use_chip NVARCHAR
        _int_or_none(row.get("merchant_id")),  # merchant_id INT
        _str_or_none(row.get("merchant_city")),# merchant_city NVARCHAR
        _str_or_none(row.get("merchant_state")),# merchant_state NVARCHAR
        _str_or_none(row.get("zip")),          # zip NVARCHAR
        _int_or_none(row.get("mcc")),          # mcc INT
        _str_or_none(row.get("errors")),       # errors NVARCHAR
    )


def load_csv(csv_path: str) -> list[dict]:
    """
    Đọc file CSV, trả về list[dict] đã sort theo cột 'date' tăng dần.
    """
    path = Path(csv_path)
    if not path.exists():
        logger.critical("Không tìm thấy file CSV: %s", csv_path)
        sys.exit(1)

    logger.info("Đang đọc CSV: %s ...", csv_path)
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    logger.info("Đọc xong %d records từ CSV.", len(rows))

    # Sắp xếp theo date tăng dần (string ISO sort được nếu format nhất quán)
    rows.sort(key=lambda r: r.get("date", "") or "")
    logger.info("Đã sắp xếp theo date tăng dần.")
    return rows


def _parse_sql_values_line(line: str) -> list[dict] | None:
    """
    Parse một dòng VALUES (...), (...) từ file SQL nguồn.
    Trả về list[dict] các record, hoặc None nếu dòng không phải VALUES.
    """
    # Tách phần VALUES từ dòng INSERT ... VALUES (...),...
    # Giữ nguyên dấu ngoặc của tuple đầu/cuối, nếu không findall bên dưới sẽ bỏ sót 2 record mỗi câu INSERT
    match = re.search(r"VALUES\s*(\(.+\))\s*;?\s*$", line, re.IGNORECASE | re.DOTALL)
    if not match:
        return None

    # Tách từng tuple record bằng regex - xử lý N'...' strings có dấu phẩy bên trong
    values_str = match.group(1)

    # Tìm tất cả các tuple: (...), (...)
    tuples = re.findall(r"\(([^()]+)\)", values_str)
    records = []
    for t in tuples:
        parts = _split_sql_tuple(t)
        if len(parts) != len(COLUMNS):
            logger.warning("Bỏ qua tuple có %d fields (cần %d): %s", len(parts), len(COLUMNS), t[:80])
            continue
        rec = dict(zip(COLUMNS, parts))
        records.append(rec)
    return records if records else None


def _split_sql_tuple(s: str) -> list[str]:
    """
    Tách chuỗi value tuple SQL thành list các giá trị thô.
    Xử lý N'...' strings có thể chứa dấu phẩy.
    """
    parts = []
    current = []
    in_string = False
    i = 0
    while i < len(s):
        c = s[i]
        if not in_string:
            if (c == "N" or c == "n") and i + 1 < len(s) and s[i + 1] == "'":
                in_string = True
                i += 2
                current.append("N'")
                continue
            elif c == "'":
                in_string = True
                current.append(c)
            elif c == ",":
                raw = "".join(current).strip()
                parts.append(_clean_sql_value(raw))
                current = []
            else:
                current.append(c)
        else:
            if c == "'" and i + 1 < len(s) and s[i + 1] == "'":
                # Escaped quote
                current.append("''")
                i += 2
                continue
            elif c == "'":
                in_string = False
                current.append(c)
            else:
                current.append(c)
        i += 1

    # Phần tử cuối
    if current:
        raw = "".join(current).strip()
        parts.append(_clean_sql_value(raw))

    return parts


def _clean_sql_value(v: str) -> str:
    """Làm sạch giá trị SQL thô: bỏ N'...', 'NULL', dấu nháy."""
    v = v.strip()
    if v.upper() == "NULL":
        return ""
    # N'...' hoặc '...'
    m = re.match(r"^N?'(.*)'$", v, re.DOTALL)
    if m:
        return m.group(1).replace("''", "'")
    return v


def load_sql_file(sql_path: str) -> list[dict]:
    """
    Đọc file SQL INSERT, parse ra list[dict] đã sort theo date tăng dần.
    Hỗ trợ định dạng multi-row VALUES của SQL Server.
    """
    path = Path(sql_path)
    if not path.exists():
        logger.critical("Không tìm thấy file SQL: %s", sql_path)
        sys.exit(1)

    logger.info("Đang đọc SQL file: %s ...", sql_path)
    rows = []
    insert_prefix = "INSERT INTO"
    buffer_lines = []

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if line.strip().upper().startswith(insert_prefix.upper()):
                buffer_lines = [line]
            elif buffer_lines:
                buffer_lines.append(line)
                if line.strip().endswith(";"):
                    full_stmt = " ".join(buffer_lines)
                    parsed = _parse_sql_values_line(full_stmt)
                    if parsed:
                        rows.extend(parsed)
                    buffer_lines = []

        # Xử lý buffer cuối chưa có dấu `;`
        if buffer_lines:
            full_stmt = " ".join(buffer_lines)
            parsed = _parse_sql_values_line(full_stmt)
            if parsed:
                rows.extend(parsed)

    logger.info("Đọc xong %d records từ SQL file.", len(rows))
    rows.sort(key=lambda r: r.get("date", "") or "")
    logger.info("Đã sắp xếp theo date tăng dần.")
    return rows


def detect_and_load_source(source_path: str) -> list[dict]:
    """
    Tự động phát hiện loại file (CSV hoặc SQL) và load.
    """
    p = Path(source_path)
    suffix = p.suffix.lower()
    if suffix == ".csv":
        return load_csv(source_path)
    elif suffix == ".sql":
        return load_sql_file(source_path)
    else:
        # Thử đọc vài bytes để phán đoán
        with open(p, "r", encoding="utf-8") as f:
            header = f.read(200).strip()
        if header.upper().startswith("INSERT"):
            return load_sql_file(source_path)
        else:
            return load_csv(source_path)


# ═══════════════════════════════════════════════════════════════════════════════
# Core: Insert batch
# ═══════════════════════════════════════════════════════════════════════════════

def insert_batch(
    cursor: pyodbc.Cursor,
    batch: list[dict],
    schema: str,
    dry_run: bool = False,
) -> tuple[int, list[tuple[dict, str]]]:
    """
    Insert một batch records vào bảng transactions.

    Args:
        cursor: pyodbc cursor đang mở.
        batch: Danh sách dict records cần insert.
        schema: Schema name (ví dụ "dbo").
        dry_run: Nếu True, chỉ log không insert thực.

    Returns:
        (số record insert thành công, danh sách (record, lý do) bị SQL Server từ chối).
    """
    sql = INSERT_SQL.format(schema=schema)
    inserted = 0
    rejected: list[tuple[dict, str]] = []

    for rec in batch:
        params = _coerce_row(rec)
        if dry_run:
            logger.info("[DRY-RUN] Would insert: id=%s, date=%s", params[0], params[1])
            inserted += 1
            continue
        try:
            cursor.execute(sql, params)
            inserted += 1
        except pyodbc.IntegrityError as exc:
            # Duplicate key → bỏ qua (có thể đã insert trước khi checkpoint)
            err_str = str(exc)
            if "2627" in err_str or "2601" in err_str or "PRIMARY KEY" in err_str.upper():
                logger.debug("Bỏ qua duplicate id=%s: %s", params[0], exc)
            else:
                # Vi phạm FK / CHECK: không insert được nhưng phải giữ lại để rà soát (DLQ)
                logger.warning("IntegrityError id=%s → DLQ: %s", params[0], exc)
                rejected.append((rec, err_str))
        except pyodbc.Error as exc:
            logger.error("Lỗi insert id=%s: %s", params[0], exc)
            raise  # Re-raise để caller xử lý rollback

    return inserted, rejected


def write_rejected(rejected_path: str, rejected: list[tuple[dict, str]]) -> None:
    """Ghi record bị từ chối vào Dead Letter Queue (JSON Lines) kèm lý do."""
    if not rejected:
        return
    path = Path(rejected_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rejected_at = datetime.now(timezone.utc).isoformat()
    with open(path, "a", encoding="utf-8") as f:
        for rec, reason in rejected:
            f.write(json.dumps(
                {"record": rec, "error": reason, "rejected_at": rejected_at},
                ensure_ascii=False,
            ) + "\n")


_last_push_warning_at = 0.0


def push_ingestion_metrics(state: dict, total_rows: int) -> None:
    """
    Đẩy tiến độ ingestion lên Prometheus Pushgateway (mục 3.7) nếu có PUSHGATEWAY_URL.
    Lỗi mạng chỉ cảnh báo (tối đa 1 lần/phút), không làm dừng ingestion.
    """
    global _last_push_warning_at
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    metrics = {
        "ingestion_inserted_records_total": state.get("total_inserted", 0),
        "ingestion_rejected_records_total": state.get("total_rejected", 0),
        "ingestion_progress_ratio": (state.get("last_row_index", -1) + 1) / total_rows if total_rows else 0,
        "ingestion_last_batch_timestamp_seconds": time.time(),
    }
    body = "".join(f"{name} {float(value)}\n" for name, value in metrics.items()).encode("utf-8")
    try:
        request = urllib.request.Request(f"{url.rstrip('/')}/metrics/job/ingestion", data=body, method="PUT")
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as exc:
        if time.time() - _last_push_warning_at > 60:
            logger.warning("Không đẩy được metrics lên Pushgateway (%s): %s", url, exc)
            _last_push_warning_at = time.time()


# ═══════════════════════════════════════════════════════════════════════════════
# Core: Main ingestion loop
# ═══════════════════════════════════════════════════════════════════════════════

def run_ingestion(
    source_path: str,
    checkpoint_path: str,
    batch_size: int,
    interval: float,
    schema: str,
    dry_run: bool = False,
    limit: int | None = None,
    rejected_path: str = REJECTED_FILE_PATH,
) -> None:
    """
    Vòng lặp chính: load dữ liệu → resume từ checkpoint → insert theo batch.
    """
    from config import DB_SCHEMA  # import ở đây để tránh circular

    # 1. Load dữ liệu nguồn
    all_rows = detect_and_load_source(source_path)
    total_rows = len(all_rows)
    if total_rows == 0:
        logger.warning("Không có record nào trong file nguồn. Dừng.")
        return

    # 2. Load checkpoint
    state = load_checkpoint(checkpoint_path)
    state["source_file"] = source_path

    # Xác định vị trí bắt đầu (0-based row index)
    start_index = state.get("last_row_index", -1) + 1

    # Checkpoint lưu theo row index: chỉ resume được khi dòng tại index đó vẫn là
    # last_transaction_id. Nếu file nguồn hoặc cách parse thay đổi, index bị lệch
    # và resume sẽ bỏ sót record một cách âm thầm.
    ckpt_tx_id = state.get("last_transaction_id")
    if start_index > 0 and ckpt_tx_id is not None:
        checkpoint_row = all_rows[start_index - 1] if start_index <= total_rows else None
        if checkpoint_row is None or str(checkpoint_row.get("id")) != str(ckpt_tx_id):
            logger.critical(
                "Checkpoint không khớp dữ liệu nguồn (row_index=%d, last_transaction_id=%s, total_rows=%d). "
                "Chạy lại với --reset (record đã insert sẽ được bỏ qua nhờ PRIMARY KEY).",
                start_index - 1,
                ckpt_tx_id,
                total_rows,
            )
            sys.exit(1)

    if state.get("status") == "completed":
        logger.info("Ingestion đã hoàn tất trước đó (status=completed). Dùng --reset để chạy lại.")
        return

    if start_index >= total_rows:
        logger.info(
            "start_index=%d >= total_rows=%d. Không còn record để insert.",
            start_index,
            total_rows,
        )
        mark_completed(checkpoint_path, state)
        return

    logger.info(
        "=== Bắt đầu ingestion | source=%s | rows=%d | resume_from=%d | batch=%d | interval=%.1fs ===",
        Path(source_path).name,
        total_rows,
        start_index,
        batch_size,
        interval,
    )

    # 3. Kết nối DB
    conn = None if dry_run else get_connection()

    rows_to_process = all_rows[start_index:]
    if limit is not None:
        rows_to_process = rows_to_process[:limit]
        logger.info("--limit %d: chỉ insert tối đa %d record.", limit, limit)

    current_row_index = start_index
    total_inserted = state.get("total_inserted", 0)

    try:
        for batch_start in range(0, len(rows_to_process), batch_size):
            if _shutdown_requested:
                logger.info("Shutdown requested. Dừng vòng lặp.")
                break

            batch = rows_to_process[batch_start : batch_start + batch_size]
            batch_end_index = current_row_index + len(batch) - 1
            last_tx_id = batch[-1].get("id")

            logger.info(
                "Batch #%d | rows[%d-%d] | tx_id cuối: %s | đang insert %d records...",
                state.get("batches_completed", 0) + 1,
                current_row_index,
                batch_end_index,
                last_tx_id,
                len(batch),
            )

            # ── Insert với retry ───────────────────────────────────────────
            inserted_count = 0
            rejected: list[tuple[dict, str]] = []
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if dry_run:
                        cursor = None
                        inserted_count, rejected = insert_batch(None, batch, schema, dry_run=True)
                    else:
                        cursor = conn.cursor()
                        inserted_count, rejected = insert_batch(cursor, batch, schema, dry_run=False)
                        conn.commit()
                        cursor.close()
                    break  # Thành công

                except pyodbc.Error as exc:
                    logger.error(
                        "Lỗi DB tại batch (attempt %d/%d): %s", attempt, MAX_RETRIES, exc
                    )
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                    if attempt < MAX_RETRIES:
                        logger.info("Đang reconnect sau %.1fs...", RETRY_DELAY_SECONDS)
                        time.sleep(RETRY_DELAY_SECONDS)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = get_connection()
                    else:
                        # Checkpoint đã được lưu sau batch thành công gần nhất; không ghi đè
                        # để giữ cặp (last_row_index, last_transaction_id) nhất quán.
                        logger.critical(
                            "Batch thất bại sau %d lần. Checkpoint giữ nguyên tại row_index=%d. Dừng.",
                            MAX_RETRIES,
                            current_row_index - 1,
                        )
                        sys.exit(1)

            # ── Ghi DLQ (chỉ sau khi batch đã commit) rồi cập nhật checkpoint ──
            write_rejected(rejected_path, rejected)
            total_inserted += inserted_count
            state = save_checkpoint(
                checkpoint_path,
                state,
                last_transaction_id=int(last_tx_id) if last_tx_id else None,
                last_row_index=batch_end_index,
                rows_inserted_this_batch=inserted_count,
                rows_rejected_this_batch=len(rejected),
            )
            push_ingestion_metrics(state, total_rows)

            current_row_index += len(batch)

            # Progress
            progress_pct = (current_row_index / total_rows) * 100
            logger.info(
                "✓ Batch #%d xong | inserted=%d | rejected=%d | total=%d/%d (%.1f%%) | last_tx_id=%s",
                state["batches_completed"],
                inserted_count,
                len(rejected),
                total_inserted,
                total_rows,
                progress_pct,
                state["last_transaction_id"],
            )

            # ── Nghỉ giữa batch ───────────────────────────────────────────
            if batch_start + batch_size < len(rows_to_process) and not _shutdown_requested:
                logger.debug("Nghỉ %.1f giây trước batch tiếp theo...", interval)
                time.sleep(interval)

    finally:
        if conn and not dry_run:
            try:
                conn.close()
                logger.info("Đã đóng kết nối DB.")
            except Exception:
                pass

    # 4. Kiểm tra hoàn tất
    if state.get("total_rejected", 0):
        logger.warning(
            "Có %d record bị SQL Server từ chối, xem chi tiết tại %s",
            state["total_rejected"],
            rejected_path,
        )
    if not _shutdown_requested:
        if current_row_index >= total_rows:
            logger.info(
                "=== HOÀN TẤT INGESTION | total_inserted=%d | total_rejected=%d | batches=%d ===",
                state["total_inserted"],
                state.get("total_rejected", 0),
                state["batches_completed"],
            )
            mark_completed(checkpoint_path, state)
        else:
            logger.info(
                "Dừng giữa chừng (--limit hoặc signal). Đã insert %d records. "
                "Chạy lại để tiếp tục.",
                state["total_inserted"],
            )
    else:
        logger.info(
            "Dừng do signal. Checkpoint đã lưu tại row_index=%d. Chạy lại để resume.",
            state["last_row_index"],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay transactions từ file nguồn vào SQL Server theo batch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Xóa checkpoint và bắt đầu lại từ đầu.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Không thực sự insert, chỉ in record sẽ được insert.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        metavar="N",
        help=f"Số record mỗi batch (mặc định: {BATCH_SIZE}).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        metavar="S",
        help=f"Khoảng nghỉ giữa batch (giây, mặc định: {BATCH_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Chỉ insert N record rồi dừng (dùng để test).",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=CSV_FILE_PATH,
        metavar="PATH",
        help=f"Đường dẫn file nguồn (mặc định: {CSV_FILE_PATH}).",
    )
    parser.add_argument(
        "--checkpoint",
        type=str,
        default=CHECKPOINT_FILE_PATH,
        metavar="PATH",
        help=f"Đường dẫn file checkpoint JSON (mặc định: {CHECKPOINT_FILE_PATH}).",
    )
    parser.add_argument(
        "--rejected",
        type=str,
        default=REJECTED_FILE_PATH,
        metavar="PATH",
        help=f"File DLQ (JSON Lines) cho record bị từ chối (mặc định: {REJECTED_FILE_PATH}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Resolve giá trị từ args hoặc config
    batch_size = args.batch_size if args.batch_size is not None else BATCH_SIZE
    interval = args.interval if args.interval is not None else BATCH_INTERVAL_SECONDS

    logger.info("─" * 60)
    logger.info("replay_transactions.py khởi động")
    logger.info("  source      : %s", args.source)
    logger.info("  checkpoint  : %s", args.checkpoint)
    logger.info("  rejected    : %s", args.rejected)
    logger.info("  batch_size  : %d", batch_size)
    logger.info("  interval    : %.1fs", interval)
    logger.info("  dry_run     : %s", args.dry_run)
    logger.info("  reset       : %s", args.reset)
    logger.info("  limit       : %s", args.limit)
    logger.info("─" * 60)

    # Import DB_SCHEMA từ config
    from config import DB_SCHEMA

    # Reset checkpoint nếu được yêu cầu
    if args.reset:
        reset_checkpoint(args.checkpoint)
        # Chạy lại từ đầu sẽ từ chối lại đúng các record cũ → xóa DLQ để không bị ghi trùng
        Path(args.rejected).unlink(missing_ok=True)
        logger.info("Checkpoint và DLQ đã được xóa. Bắt đầu từ đầu.")

    run_ingestion(
        source_path=args.source,
        checkpoint_path=args.checkpoint,
        batch_size=batch_size,
        interval=interval,
        schema=DB_SCHEMA,
        dry_run=args.dry_run,
        limit=args.limit,
        rejected_path=args.rejected,
    )


if __name__ == "__main__":
    main()
