"""
checkpoint.py
-------------
Quản lý trạng thái checkpoint cho quá trình ingestion.
Lưu và đọc vị trí đã insert (last_transaction_id, last_row_index)
vào file JSON để có thể resume khi script bị dừng giữa chừng.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cấu trúc mặc định của checkpoint state
DEFAULT_STATE: dict = {
    "last_transaction_id": None,   # transaction_id cuối cùng đã insert thành công
    "last_row_index": -1,           # Index dòng (0-based) cuối cùng đã insert
    "total_inserted": 0,            # Tổng số record đã insert từ đầu
    "total_rejected": 0,            # Tổng số record bị từ chối (ghi vào DLQ)
    "batches_completed": 0,         # Số batch đã hoàn thành
    "started_at": None,             # Thời điểm bắt đầu chạy lần đầu
    "last_updated_at": None,        # Thời điểm cập nhật checkpoint gần nhất
    "source_file": None,            # Đường dẫn file nguồn đang được xử lý
    "status": "not_started",        # "not_started" | "in_progress" | "completed"
}


def load_checkpoint(checkpoint_path) -> dict:
    """
    Đọc checkpoint từ file JSON.
    Nếu file không tồn tại hoặc bị lỗi, trả về DEFAULT_STATE (bắt đầu từ đầu).
    """
    path = Path(checkpoint_path)

    if not path.exists():
        logger.info("Checkpoint file không tồn tại. Bắt đầu từ đầu.")
        return DEFAULT_STATE.copy()

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                logger.info("Checkpoint file rỗng. Bắt đầu từ đầu.")
                return DEFAULT_STATE.copy()
            state = json.loads(content)

        # Merge với DEFAULT_STATE để đảm bảo các key mới luôn có mặt
        merged = DEFAULT_STATE.copy()
        merged.update(state)

        logger.info(
            "Đã load checkpoint: last_row_index=%d, total_inserted=%d, status=%s",
            merged["last_row_index"],
            merged["total_inserted"],
            merged["status"],
        )
        return merged

    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Không thể đọc checkpoint file (%s). Reset về đầu.", exc)
        return DEFAULT_STATE.copy()


def save_checkpoint(
    checkpoint_path,
    state: dict,
    last_transaction_id: Optional[int] = None,
    last_row_index: Optional[int] = None,
    rows_inserted_this_batch: int = 0,
    rows_rejected_this_batch: int = 0,
) -> dict:
    """
    Cập nhật và lưu checkpoint vào file JSON (atomic write qua temp file).
    """
    path = Path(checkpoint_path)

    if last_transaction_id is not None:
        state["last_transaction_id"] = last_transaction_id
    if last_row_index is not None:
        state["last_row_index"] = last_row_index

    state["total_inserted"] = state.get("total_inserted", 0) + rows_inserted_this_batch
    state["total_rejected"] = state.get("total_rejected", 0) + rows_rejected_this_batch
    state["batches_completed"] = state.get("batches_completed", 0) + 1
    state["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    state["status"] = "in_progress"

    if state.get("started_at") is None:
        state["started_at"] = state["last_updated_at"]

    # Atomic write: ghi ra temp file rồi rename để tránh corrupt khi bị kill
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=path.parent, prefix=".checkpoint_tmp_", suffix=".json"
        )
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)  # atomic trên cùng filesystem

        logger.debug(
            "Checkpoint saved: last_row_index=%d, last_tx_id=%s, total=%d",
            state["last_row_index"],
            state["last_transaction_id"],
            state["total_inserted"],
        )
    except OSError as exc:
        logger.error("Không thể lưu checkpoint: %s", exc)

    return state


def mark_completed(checkpoint_path, state: dict) -> dict:
    """
    Đánh dấu quá trình ingestion đã hoàn tất.
    """
    state["status"] = "completed"
    state["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    path = Path(checkpoint_path)

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=path.parent, prefix=".checkpoint_tmp_", suffix=".json"
        )
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)  # atomic như save_checkpoint
        logger.info("Checkpoint marked as COMPLETED.")
    except OSError as exc:
        logger.error("Không thể lưu checkpoint hoàn tất: %s", exc)

    return state


def reset_checkpoint(checkpoint_path) -> dict:
    """
    Xóa checkpoint (reset về trạng thái ban đầu).
    """
    path = Path(checkpoint_path)
    if path.exists():
        path.unlink()
        logger.info("Checkpoint đã được reset.")
    return DEFAULT_STATE.copy()
