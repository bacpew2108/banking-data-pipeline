# banking-data-pipeline

Pipeline dữ liệu giao dịch thẻ ngân hàng theo kiến trúc Medallion:

```
SQL Server ──(Debezium CDC)──▶ Kafka ──(Spark Streaming)──▶ MinIO Bronze ──(Spark)──▶ MinIO Silver
                                                                                         │
            Superset ◀── PostgreSQL Gold (dbt Star Schema) ◀── Staging ◀── Quality Gate (Great Expectations)
```

- **Ingestion** (`ingestion/`): replay file giao dịch vào SQL Server theo batch, có checkpoint để resume và DLQ cho bản ghi bị từ chối.
- **Bronze / Silver** (`spark/jobs/`): Spark Structured Streaming; Silver làm sạch, khử trùng lặp, mask PII, tách DLQ.
- **Quality Gate** (`great_expectations/`): kiểm tra Silver trước khi dbt chạy; FAIL thì chặn tầng Gold.
- **Gold** (`dbt/`): star schema (dim/fact) và các data mart.
- **Điều phối** (`airflow/dags/`): DAG `silver_quality_to_gold_pipeline` chạy mỗi 30 phút.
- **Giám sát** (`monitoring/`): Prometheus, Pushgateway, cAdvisor, Alertmanager, Grafana.

Chi tiết thiết kế: [Banking_doc.md](Banking_doc.md).

## Cài đặt

1. Cài Docker Desktop (khuyến nghị cấp ≥ 8 GB RAM).
2. Tạo file cấu hình và điền mọi giá trị `<...>`:

   ```bash
   cp .env.example .env
   ```

   Lệnh sinh chuỗi ngẫu nhiên có ghi chú ngay trong `.env.example`. Không commit file `.env`.

## Chạy

```bash
docker compose up -d --build
```

Các service khởi tạo tự động theo thứ tự: tạo database + bật CDC (`sqlserver-init`), tạo bucket có mã hóa (`minio-init`), đăng ký connector Debezium (`debezium-init`), tạo role chỉ-đọc cho Superset (`postgres/init/`), rồi mới bật Spark streaming và ingestion.

**Chạy lại từ đầu** (xóa toàn bộ dữ liệu trong các volume):

```bash
docker compose down -v
rm -f ingestion/checkpoint_state.json
docker compose up -d --build
```

Ingestion tự resume từ `ingestion/checkpoint_state.json`. Nếu checkpoint không khớp dữ liệu nguồn, ingestion dừng và yêu cầu chạy lại với `--reset`:

```bash
docker compose run --rm ingestion python ingestion/replay_transactions.py --reset
```

## Giao diện

| Dịch vụ | Địa chỉ | Tài khoản |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Superset | http://localhost:8088 | `admin` / `SUPERSET_ADMIN_PASSWORD` |
| Grafana | http://localhost:3001 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Spark Master UI | http://localhost:8081 | |
| Prometheus | http://localhost:9090 | |
| Pushgateway | http://localhost:9091 | |
| Alertmanager | http://localhost:9093 | |
| Debezium Connect REST | http://localhost:8083 | |

## Dữ liệu lỗi ở đâu

- Bản ghi bị SQL Server từ chối khi ingestion (vi phạm khóa ngoại...): `ingestion/dlq/rejected_transactions.jsonl`
- Bản ghi không qua kiểm tra chất lượng ở Silver: `s3a://silver/dlq/transactions_rejected/`
- Báo cáo Great Expectations: `great_expectations/gx/uncommitted/data_docs/local_site/index.html`

## Lưu ý

- `MINIO_KMS_SECRET_KEY` là khóa mã hóa dữ liệu trong MinIO: đổi khóa thì không đọc được dữ liệu cũ.
- Alertmanager hiện chỉ hiển thị cảnh báo trên UI; mẫu cấu hình gửi Telegram / Slack / Email có sẵn trong `monitoring/alertmanager/alertmanager.yml`.
- Trên Docker Desktop dùng containerd image store, cAdvisor không đọc được tên container; dashboard hiển thị theo `container_id` (12 ký tự đầu, tra bằng `docker ps`).

## Môi trường Python local (tùy chọn)

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
