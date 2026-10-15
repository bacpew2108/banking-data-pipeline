"""
pipeline_metrics.py
Sinh lệnh bash đẩy trạng thái lần chạy DAG lên Prometheus Pushgateway (mục 3.7 - Airflow DAG Status).
Prometheus dùng 2 metric này để cảnh báo DAG thất bại hoặc lâu không chạy.
"""


def push_dag_status_command(dag_id: str, success: str) -> str:
    """
    success: biểu thức bash cho giá trị 1/0, vd "1" hoặc "$status".
    Lỗi khi đẩy metric chỉ in cảnh báo, không làm fail task.
    """
    return (
        f'printf "airflow_dag_last_run_success %s\\nairflow_dag_last_run_timestamp_seconds %s\\n" '
        f'"{success}" "$(date +%s)" '
        f'| curl -sS --fail -X PUT --data-binary @- '
        f'"${{PUSHGATEWAY_URL:-http://pushgateway:9091}}/metrics/job/airflow_dag/dag_id/{dag_id}" '
        f'|| echo "! Không đẩy được trạng thái DAG lên Pushgateway"'
    )
