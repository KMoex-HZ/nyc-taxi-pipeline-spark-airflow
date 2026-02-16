from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- DAG Definition & Configuration ---
# Standard metadata and retry policies for production-grade pipelines
default_args = {
    'owner': 'caelan_zhou',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_data_pipeline_v_final',
    default_args=default_args,
    description='End-to-End Data Pipeline: Ingestion (MinIO) -> Transformation (Spark) -> Data Quality Validation',
    schedule_interval='@monthly', 
    catchup=False,
    tags=['engineering', 'etl', 'nyc_taxi']
) as dag:

    # TASK 1: Data Ingestion
    # Executes the ingestion script to fetch raw data and store it in S3-compatible storage (MinIO)
    task_ingest = BashOperator(
        task_id='ingest_to_minio',
        bash_command='python3 /opt/airflow/scripts/ingest_to_minio.py'
    )

    # TASK 2: Distributed Data Transformation
    # Triggers Spark Submit to process large-scale taxi data using the Spark cluster
    task_transform = BashOperator(
        task_id='spark_transformation',
        bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.5.0 \
        /opt/bitnami/spark/apps/transform_taxi_data.py
        """
    )

    # TASK 3: Data Quality Assurance
    # Performs hybrid data quality checks to ensure integrity before downstream consumption
    task_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='python3 /opt/airflow/scripts/data_quality_check.py'
    )

    # --- Pipeline Workflow Orchestration ---
    # Sequential Execution: Ingestion -> Transformation -> Validation
    task_ingest >> task_transform >> task_quality_check