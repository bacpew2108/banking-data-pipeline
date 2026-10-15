"""
silver_quality_to_gold_dag.py
DAG điều phối kiểm tra chất lượng dữ liệu (Data Quality Gate) và chuyển tiếp lên tầng Gold (Mục 3.6):
1. task 'silver_transformation': Nếu daemon spark_silver_streaming đang chạy thì bỏ qua (Silver đã được nạp liên tục);
   nếu không, dựng lại Silver từ Bronze bằng Spark batch (overwrite). Không bao giờ chạy hai writer cùng lúc.
2. task 'great_expectations_quality_gate': Quét 5 tiêu chí chất lượng (Completeness, Validity, Consistency, Uniqueness, Timeliness)
3. task 'dbt_transform_gold': Nếu PASS chất lượng, dbt sẽ tổng hợp dữ liệu Silver lên Gold (Star Schema)
4. task 'quality_failure_alert': Nếu FAIL, chặn dbt, gửi cảnh báo cách ly dữ liệu lỗi; DAG run được đánh dấu failed
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from pipeline_metrics import push_dag_status_command

DAG_ID = "silver_quality_to_gold_pipeline"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Điều phối Silver Transformation, Great Expectations Quality Gate (Mục 3.6) và dbt Gold",
    schedule="*/30 * * * *",  # Chạy định kỳ mỗi 30 phút (hoặc theo chu kỳ mong muốn)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Không để 2 lần chạy cùng ghi Silver / Staging
    is_paused_upon_creation=False,  # Tự động unpause và chạy theo lịch
    tags=["silver", "gold", "great_expectations", "quality_gate", "dbt"],
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    # ── Task 1: Silver Transformation (batch rebuild chỉ khi streaming không chạy) ──
    # Batch rebuild ghi đè toàn bộ Silver; chạy song song với streaming sẽ làm hai bên giẫm lên nhau.
    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        set -euo pipefail
        echo "[1/3] Kiểm tra tầng Silver..."
        if [ -n "$(docker ps -q --filter 'name=^spark_silver_streaming$' --filter status=running)" ]; then
          echo "Silver đang được nạp liên tục bởi spark_silver_streaming. Bỏ qua batch để tránh ghi đồng thời."
        else
          echo "Không có streaming Silver đang chạy. Dựng lại Silver từ Bronze (batch, overwrite)..."
          docker exec spark-master /opt/spark/bin/spark-submit \
            --master "local[1]" \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/spark/apps/jobs/silver/stream_silver_transformation.py --mode batch
        fi
        """
    )

    # ── Task 2: Great Expectations Quality Gate (Kiểm tra 5 tiêu chí mục 3.6) ──
    # Chạy lại container 'great_expectations' do docker compose tạo sẵn (đã có mount, network, .env
    # theo đúng thư mục dự án), nên không phụ thuộc đường dẫn tuyệt đối trên máy host.
    # `docker start --attach` trả về exit code của container: vi phạm -> exit 1 -> Task Fail.
    quality_gate = BashOperator(
        task_id="great_expectations_quality_gate",
        bash_command="""
        set -euo pipefail
        echo "[2/3] Chạy Great Expectations Quality Gate quét 5 tiêu chí..."
        docker start --attach great_expectations
        """
    )

    # ── Task 3: Đồng bộ Silver → Staging + Kích hoạt dbt khi đạt chuẩn Data Quality ──
    dbt_transform_gold = BashOperator(
        task_id="dbt_transform_gold",
        bash_command="""
        set -euo pipefail
        echo "[3/3] Dữ liệu đạt chuẩn chất lượng. Đang đồng bộ Silver → Staging và kích hoạt dbt Gold..."

        echo "── Bước 3a: Nạp Silver Parquet (MinIO) → PostgreSQL Staging ──"
        docker exec dbt python /usr/app/dbt/load_silver_to_staging.py

        echo "── Bước 3b: dbt run - Xây dựng Star Schema (Dimensions, Fact, Data Marts) ──"
        docker exec dbt dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt

        echo "── Bước 3c: dbt test - Kiểm tra tính toàn vẹn dữ liệu (FK, unique, not_null) ──"
        docker exec dbt dbt test --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt
        """
    )

    # ── Task 4: Xử lý khi pipeline thất bại (Silver / Quality Gate / dbt) ──
    # Đẩy trạng thái thất bại lên Pushgateway để Prometheus/Alertmanager cảnh báo (mục 3.7).
    quality_failure_alert = BashOperator(
        task_id="quality_failure_alert",
        bash_command=f"""
        echo "[CẢNH BÁO] Pipeline Silver → Gold thất bại (Silver transform / Quality Gate / dbt)!"
        echo "Pipeline đã CHẶN không cho dữ liệu lỗi nạp vào Data Warehouse (Gold Layer)."
        echo "Kiểm tra log task lỗi và các bản ghi vi phạm tại s3a://silver/dlq/transactions_rejected/"
        {push_dag_status_command(DAG_ID, "0")}
        """,
        trigger_rule="one_failed"
    )

    # none_failed: chạy khi không có task nào fail. Nếu Quality Gate fail, dbt bị upstream_failed
    # -> end_pipeline cũng upstream_failed -> DAG run hiển thị FAILED (all_done sẽ luôn báo xanh).
    end_pipeline = BashOperator(
        task_id="end_pipeline",
        bash_command=f"""
        echo "Pipeline Silver → Gold hoàn tất."
        {push_dag_status_command(DAG_ID, "1")}
        """,
        trigger_rule="none_failed"
    )

    # ── Định nghĩa luồng phụ thuộc (Task Dependencies) ──
    start_pipeline >> silver_transformation >> quality_gate
    quality_gate >> dbt_transform_gold >> end_pipeline
    [quality_gate, dbt_transform_gold] >> quality_failure_alert >> end_pipeline
