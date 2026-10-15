#!/bin/bash
# Tạo role chỉ-đọc cho Superset / analyst (mục 3.1.2 - phân quyền truy cập):
# chỉ được SELECT trên schema gold, không thấy staging và không ghi được gì.
# Script chạy tự động MỘT LẦN khi volume Postgres được khởi tạo lần đầu
# (cơ chế /docker-entrypoint-initdb.d của image postgres).
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v reader="$SUPERSET_DWH_USER" \
  -v reader_pw="$SUPERSET_DWH_PASSWORD" \
  -v owner="$POSTGRES_USER" \
  -v db="$POSTGRES_DB" <<'SQL'
CREATE ROLE :"reader" LOGIN PASSWORD :'reader_pw';

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO :"reader";

-- dbt tạo bảng gold bằng tài khoản owner; default privileges giúp mọi bảng
-- dbt tạo lại sau mỗi lần chạy đều tự cấp SELECT cho reader.
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION :"owner";
GRANT USAGE ON SCHEMA gold TO :"reader";
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO :"reader";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA gold GRANT SELECT ON TABLES TO :"reader";
SQL

echo "✓ Đã tạo role chỉ-đọc '$SUPERSET_DWH_USER' (SELECT trên schema gold)"
