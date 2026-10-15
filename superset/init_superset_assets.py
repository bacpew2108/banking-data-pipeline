"""
init_superset_assets.py
Tự động cấu hình Database Connection, Datasets và Dashboard trên Apache Superset
khi container superset-init chạy.
"""

import os
import sys
import time
from urllib.parse import quote_plus

def setup_superset_assets():
    from superset.app import create_app
    app = create_app()

    with app.app_context():
        from superset import db
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard

        print("==========================================================")
        print("  BẮT ĐẦU TỰ ĐỘNG CẤU HÌNH APACHE SUPERSET (AUTO-ASSETS)  ")
        print("==========================================================")

        # Superset dùng role chỉ-đọc (chỉ SELECT schema gold), không dùng tài khoản owner của DWH.
        # Role được tạo bởi postgres/init/01_create_superset_reader.sh.
        pg_user = os.environ["SUPERSET_DWH_USER"]
        pg_pass = os.environ["SUPERSET_DWH_PASSWORD"]
        pg_db = os.environ["POSTGRES_DB"]
        pg_host = os.environ.get("PG_DWH_HOST", "data_warehouse")
        pg_port = os.environ.get("PG_DWH_PORT", "5432")

        # ── 1. Đăng ký Database Connection (Data Warehouse PostgreSQL) ──
        db_name = "Banking Data Warehouse"
        # quote_plus: mật khẩu chứa @ ! : ... không làm hỏng URI
        sqlalchemy_uri = (
            f"postgresql://{quote_plus(pg_user)}:{quote_plus(pg_pass)}@{pg_host}:{pg_port}/{pg_db}"
        )

        dwh_db = db.session.query(Database).filter_by(database_name=db_name).first()
        if not dwh_db:
            dwh_db = Database(
                database_name=db_name,
                sqlalchemy_uri=sqlalchemy_uri,
                expose_in_sqllab=True,
                allow_run_async=True,
                allow_ctas=False,  # Role chỉ-đọc không được tạo bảng/view
                allow_cvas=False,
                allow_dml=False
            )
            db.session.add(dwh_db)
            db.session.commit()
            print(f"✓ [1/3] Đã tạo kết nối Database: '{db_name}' ({sqlalchemy_uri.split('@')[1]})")
        else:
            dwh_db.sqlalchemy_uri = sqlalchemy_uri
            dwh_db.allow_ctas = False
            dwh_db.allow_cvas = False
            db.session.commit()
            print(f"✓ [1/3] Database connection '{db_name}' đã sẵn sàng.")

        # ── 2. Đăng ký các Datasets tầng Gold (Star Schema) ──
        gold_datasets = [
            {
                "table": "mart_daily_transaction",
                "schema": "gold",
                "description": "Tổng hợp giao dịch theo ngày, doanh thu và tỷ lệ lỗi giao dịch"
            },
            {
                "table": "mart_customer",
                "schema": "gold",
                "description": "Hành vi chi tiêu theo từng khách hàng (tổng chi, tần suất, điểm tín dụng)"
            },
            {
                "table": "mart_card",
                "schema": "gold",
                "description": "Phân tích doanh số theo thương hiệu (Visa/Mastercard/Amex) và chip"
            },
            {
                "table": "fact_transaction",
                "schema": "gold",
                "description": "Bảng Fact giao dịch gốc (Star Schema) kết nối 4 bảng Dimension"
            }
        ]

        for ds in gold_datasets:
            tbl = db.session.query(SqlaTable).filter_by(
                database_id=dwh_db.id,
                schema=ds["schema"],
                table_name=ds["table"]
            ).first()

            if not tbl:
                tbl = SqlaTable(
                    table_name=ds["table"],
                    schema=ds["schema"],
                    database_id=dwh_db.id,
                    description=ds["description"]
                )
                db.session.add(tbl)
                db.session.commit()
                try:
                    tbl.fetch_metadata()
                    db.session.commit()
                    print(f"✓ [2/3] Đã đăng ký Dataset: {ds['schema']}.{ds['table']} (đã nạp metadata cột)")
                except Exception as e:
                    print(f"✓ [2/3] Đã đăng ký Dataset: {ds['schema']}.{ds['table']} (chưa fetch metadata: {e})")
            else:
                try:
                    tbl.fetch_metadata()
                    db.session.commit()
                except Exception:
                    pass
                print(f"✓ [2/3] Dataset {ds['schema']}.{ds['table']} đã tồn tại.")

        # ── 3. Tạo Dashboard mẫu 'Banking Executive Analytics' ──
        dash_title = "Banking Executive Analytics"
        dash_slug = "banking-executive-analytics"
        dashboard = db.session.query(Dashboard).filter_by(slug=dash_slug).first()

        if not dashboard:
            dashboard = Dashboard(
                dashboard_title=dash_title,
                slug=dash_slug,
                published=True,
                description="Dashboard tổng hợp phân tích giao dịch ngân hàng, khách hàng và loại thẻ"
            )
            db.session.add(dashboard)
            db.session.commit()
            print(f"✓ [3/3] Đã tạo Dashboard: '{dash_title}' (slug: {dash_slug})")
        else:
            print(f"✓ [3/3] Dashboard '{dash_title}' đã tồn tại sẵn.")

        print("==========================================================")
        print("  HOÀN TẤT TỰ ĐỘNG CẤU HÌNH ASSETS CHO APACHE SUPERSET!   ")
        print("==========================================================")

if __name__ == "__main__":
    setup_superset_assets()
