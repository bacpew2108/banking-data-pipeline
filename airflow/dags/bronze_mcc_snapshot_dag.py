"""
bronze_mcc_snapshot_dag.py
DAG định kỳ hàng tuần nạp snapshot bảng tĩnh mcc_codes vào Bronze Lakehouse rồi đồng bộ sang Silver.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from pipeline_metrics import push_dag_status_command

DAG_ID = "bronze_mcc_codes_snapshot"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Batch snapshot bảng tĩnh mcc_codes vào MinIO Bronze rồi đồng bộ sang Silver",
    schedule="0 0 * * 0",  # Chạy 00:00 Chủ nhật hàng tuần
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "snapshot", "lakehouse"],
) as dag:

    # Nộp spark-submit vào Spark Master để thực thi snapshot
    submit_mcc_snapshot = BashOperator(
        task_id="submit_mcc_snapshot_job",
        bash_command=f"""
        set -uo pipefail
        status=1
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master "local[1]" \
          --conf spark.jars.ivy=/tmp/.ivy \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
          /opt/spark/apps/jobs/bronze/snapshot_mcc.py || status=0
        # Đồng bộ ngay sang Silver (streaming Silver chỉ đọc MCC lúc khởi động)
        if [ "$status" = "1" ]; then
          docker exec spark-master /opt/spark/bin/spark-submit \
            --master "local[1]" \
            --conf spark.jars.ivy=/tmp/.ivy \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/spark/apps/jobs/silver/stream_silver_transformation.py --mode mcc || status=0
        fi
        # Đẩy trạng thái lên Pushgateway (mục 3.7) rồi trả exit code đúng cho Airflow
        {push_dag_status_command(DAG_ID, "$status")}
        [ "$status" = "1" ]
        """
    )
