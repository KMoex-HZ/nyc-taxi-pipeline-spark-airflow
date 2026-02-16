# Base image using Apache Airflow with Python 3.x
FROM apache/airflow:2.7.1

USER root
# Install Docker CLI to enable task execution via Docker-out-of-Docker (DooD)
# This allows Airflow tasks to communicate with the Spark Master container
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Provisioning Python dependencies for Data Ingestion, Quality, and Database connectivity
RUN pip install --no-cache-dir \
    minio \
    requests \
    great_expectations \
    sqlalchemy \
    psycopg2-binary