import os

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db",
)

# Không dùng giá trị mặc định: SECRET_KEY yếu cho phép giả mạo session đăng nhập
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")
if not SECRET_KEY:
    raise ValueError("Thiếu biến môi trường SUPERSET_SECRET_KEY")
