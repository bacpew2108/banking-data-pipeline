FROM apache/superset:6.0.0

USER root

COPY requirements-local.txt /app/requirements-local.txt

RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages -r /app/requirements-local.txt

COPY superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset