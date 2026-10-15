FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    great-expectations==1.22.0 \
    pyarrow \
    pandas

COPY . /app/great_expectations

ENTRYPOINT ["python", "/app/great_expectations/validate_silver.py"]
