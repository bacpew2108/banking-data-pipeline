#!/bin/sh
# Tạo bucket bronze/silver và bật mã hóa at-rest SSE-S3 mặc định (mục 3.1.2).
# Chạy bởi service minio-init (dùng mc có sẵn trong image minio). Chạy lại nhiều lần vẫn an toàn.
set -eu

echo "Đang đợi MinIO sẵn sàng..."
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 2
done

for bucket in bronze silver; do
  mc mb --ignore-existing "local/$bucket"
  # Mọi object ghi vào bucket đều được MinIO mã hóa bằng khóa KMS (MINIO_KMS_SECRET_KEY)
  mc encrypt set sse-s3 "local/$bucket"
done

mc encrypt info local/bronze
mc encrypt info local/silver
echo "✓ Bucket bronze/silver đã sẵn sàng và bật SSE-S3"
