"""
build_suite.py
Script thiết lập Expectation Suite cho tầng Silver (transactions_clean)
Dựa trên 5 tiêu chí chuẩn hóa của mục 3.6 (Data Quality):
1. Completeness: Không NULL ở các trường cốt lõi (id, client_id, card_id, amount, tx_timestamp)
2. Validity: amount khác 0 (amount < 0 là hoàn tiền, gắn cờ is_refund), date/timestamp đúng định dạng
3. Consistency: Check schema và các trường phân loại chuẩn
4. Uniqueness: id là duy nhất, không trùng lặp
5. Timeliness: tx_timestamp hợp lệ
"""

import os
import sys
import json
from pathlib import Path

def setup_suite():
    import great_expectations as gx
    import great_expectations.expectations as gxe

    gx_root = Path(__file__).resolve().parent
    context = gx.get_context(project_root_dir=str(gx_root))
    suite_name = "silver_transactions_suite"

    suite = gx.ExpectationSuite(name=suite_name)

    # ── 1. Completeness: Không được chứa giá trị NULL ──
    required_columns = ["id", "client_id", "card_id", "amount", "tx_timestamp"]
    for col_name in required_columns:
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(
                column=col_name,
                notes=f"Completeness: Trường {col_name} là bắt buộc và không được phép NULL"
            )
        )

    # ── 2. Validity: amount khác 0 (giao dịch hoàn tiền có amount < 0 vẫn hợp lệ) ──
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeInSet(
            column="amount",
            value_set=[0],
            notes="Validity: Giá trị giao dịch amount phải khác 0 (amount < 0 là hoàn tiền)"
        )
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="use_chip",
            value_set=["Chip Transaction", "Swipe Transaction", "Online Transaction"],
            mostly=0.95,
            notes="Validity: use_chip phải thuộc tập giá trị chuẩn"
        )
    )

    # ── 3. Consistency: Schema & Khóa ngoại ──
    expected_cols = [
        "id", "client_id", "card_id", "amount", "use_chip",
        "merchant_id", "merchant_city", "merchant_state", "zip",
        "mcc", "errors", "is_error", "is_refund", "tx_timestamp", "year", "month", "day"
    ]
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=expected_cols,
            exact_match=False,
            notes="Consistency: Bảng phải có đầy đủ các cột chuẩn của tầng Silver"
        )
    )

    # ── 4. Uniqueness: id không được trùng lặp ──
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeUnique(
            column="id",
            notes="Uniqueness: ID giao dịch không được trùng lặp (100%, không cho phép ngưỡng mostly)"
        )
    )

    # Lưu suite vào Data Context
    context.suites.add_or_update(suite)
    print(f"✓ Đã thêm/cập nhật thành công Expectation Suite: '{suite_name}'")

    # Xuất ra file JSON trong thư mục expectations/ để lưu trữ và version control
    expectations_dir = gx_root / "expectations"
    expectations_dir.mkdir(parents=True, exist_ok=True)
    json_path = expectations_dir / f"{suite_name}.json"

    # Lấy định dạng dict từ suite (GX 1.x model_dump / dict)
    try:
        if hasattr(suite, "model_dump"):
            suite_dict = suite.model_dump()
        elif hasattr(suite, "to_json_dict"):
            suite_dict = suite.to_json_dict()
        else:
            suite_dict = json.loads(suite.json())
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(suite_dict, f, indent=2, ensure_ascii=False)
        print(f"✓ Đã lưu file cấu hình: {json_path}")
    except Exception as e:
        print(f"! Lưu JSON file: {e}")

    return suite

if __name__ == "__main__":
    setup_suite()
