"""
streaming_metrics.py
Đẩy metrics của Spark Structured Streaming lên Prometheus Pushgateway (mục 3.7):
- Throughput: số bản ghi vào / xử lý mỗi giây
- Kafka lag: số offset mà job đang chậm so với đầu topic (Spark không commit offset
  vào consumer group nên kafka-exporter không đo được lag của Spark)
- Spark Job Status: heartbeat mỗi micro-batch / lúc idle; job chết hoặc treo thì
  heartbeat ngừng cập nhật và Prometheus cảnh báo
Chỉ dùng thư viện chuẩn để chạy được trong image apache/spark mà không cài thêm gói.
"""

import math
import os
import time
import urllib.parse
import urllib.request

from pyspark.sql.streaming import StreamingQueryListener

_last_warning_at = 0.0


def push_metrics(job, grouping, metrics):
    """
    POST metrics (dạng {tên: giá trị}) lên Pushgateway dưới group job/<job>/<label>/<value>.
    POST chỉ thay các metric cùng tên trong group, giữ nguyên metric khác.
    Lỗi mạng không được làm hỏng job streaming: chỉ cảnh báo, tối đa 1 lần/phút.
    """
    global _last_warning_at
    base_url = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091").rstrip("/")
    path = f"/metrics/job/{urllib.parse.quote(job, safe='')}"
    for label, value in grouping.items():
        path += f"/{urllib.parse.quote(label, safe='')}/{urllib.parse.quote(str(value), safe='')}"

    lines = []
    for name, value in metrics.items():
        value = float(value) if value is not None else 0.0
        if math.isnan(value) or math.isinf(value):
            value = 0.0
        lines.append(f"{name} {value}")
    body = ("\n".join(lines) + "\n").encode("utf-8")

    try:
        request = urllib.request.Request(base_url + path, data=body, method="POST")
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as e:
        if time.time() - _last_warning_at > 60:
            print(f"! Không đẩy được metrics lên Pushgateway ({base_url}): {e}", flush=True)
            _last_warning_at = time.time()


class PushgatewayListener(StreamingQueryListener):
    """Listener gắn vào SparkSession: mỗi query đẩy metrics vào group job/<job>/query/<tên query>."""

    def __init__(self, job):
        self.job = job
        self.query_names = {}  # id query -> tên query (event idle/terminated không có tên)
        self.input_rows_total = {}  # tên query -> tổng số bản ghi đã đọc kể từ khi driver khởi động

    def _name(self, query_id):
        return self.query_names.get(str(query_id), str(query_id))

    def onQueryStarted(self, event):
        name = event.name or str(event.id)
        self.query_names[str(event.id)] = name
        push_metrics(self.job, {"query": name}, {
            "spark_streaming_query_active": 1,
            "spark_streaming_last_heartbeat_timestamp_seconds": time.time(),
        })

    def onQueryProgress(self, event):
        progress = event.progress
        name = progress.name or self._name(progress.id)
        self.input_rows_total[name] = self.input_rows_total.get(name, 0) + progress.numInputRows

        metrics = {
            "spark_streaming_query_active": 1,
            "spark_streaming_input_rows_total": self.input_rows_total[name],
            "spark_streaming_input_rows_per_second": progress.inputRowsPerSecond,
            "spark_streaming_processed_rows_per_second": progress.processedRowsPerSecond,
            "spark_streaming_batch_duration_seconds": progress.batchDuration / 1000.0,
            "spark_streaming_last_heartbeat_timestamp_seconds": time.time(),
        }
        # Kafka source tự tính khoảng cách tới offset mới nhất của topic
        offsets_behind = [
            float(source.metrics["maxOffsetsBehindLatest"])
            for source in progress.sources
            if source.metrics and "maxOffsetsBehindLatest" in source.metrics
        ]
        if offsets_behind:
            metrics["spark_streaming_kafka_offsets_behind_latest"] = max(offsets_behind)
        push_metrics(self.job, {"query": name}, metrics)

    def onQueryIdle(self, event):
        # Không có dữ liệu mới nhưng query vẫn sống: chỉ cập nhật heartbeat
        push_metrics(self.job, {"query": self._name(event.id)}, {
            "spark_streaming_query_active": 1,
            "spark_streaming_last_heartbeat_timestamp_seconds": time.time(),
        })

    def onQueryTerminated(self, event):
        push_metrics(self.job, {"query": self._name(event.id)}, {
            "spark_streaming_query_active": 0,
        })
