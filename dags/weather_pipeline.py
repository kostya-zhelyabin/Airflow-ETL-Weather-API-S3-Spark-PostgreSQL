from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

BUCKET = "weather-data"
RAW_KEY = (
    "raw/weatherapi/dt={{ (data_interval_start or dag_run.run_after).strftime('%Y-%m-%d') }}"
    "/hour={{ (data_interval_start or dag_run.run_after).strftime('%H') }}/weather.json"
)


@dag(
    dag_id="weather_api_s3_spark_postgres",
    description="WeatherAPI.com -> MinIO/S3 -> Spark -> PostgreSQL",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["learning", "weather", "spark"],
)
def weather_pipeline():
    @task
    def extract_to_s3(object_key: str) -> str:
        import requests

        s3 = S3Hook(aws_conn_id="aws_default")
        raw_path = f"s3a://{BUCKET}/{object_key}"
        if s3.check_for_key(key=object_key, bucket_name=BUCKET):
            logger.info("Reusing existing weather payload: %s", raw_path)
            return raw_path

        api_key = os.environ.get("WEATHERAPI_KEY", "").strip()
        if not api_key:
            raise ValueError("Set WEATHERAPI_KEY in the local .env file")

        try:
            response = requests.get(
                "https://api.weatherapi.com/v1/current.json",
                params={"key": api_key, "q": "55.7558,37.6173"},
                timeout=(10, 30),
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"WeatherAPI request failed: {type(exc).__name__}") from None

        if not response.ok:
            raise RuntimeError(f"WeatherAPI returned HTTP {response.status_code}")
        s3.load_string(
            string_data=json.dumps(response.json(), ensure_ascii=False),
            key=object_key,
            bucket_name=BUCKET,
            replace=False,
        )
        logger.info("Saved weather payload: %s", raw_path)
        return raw_path

    create_table = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="weather_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_observations (
            observed_at TIMESTAMPTZ PRIMARY KEY,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            temperature_c DOUBLE PRECISION,
            relative_humidity_pct DOUBLE PRECISION,
            wind_speed_kmh DOUBLE PRECISION,
            source_file TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    )

    transform_load = SparkSubmitOperator(
        task_id="spark_transform_and_load",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/weather_job.py",
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='extract_to_s3') }}",
            "--jdbc-url", "jdbc:postgresql://postgres:5432/weather",
            "--db-user", os.environ["POSTGRES_USER"],
            "--db-password", os.environ["POSTGRES_PASSWORD"],
        ],
        packages=(
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "org.postgresql:postgresql:42.7.9"
        ),
        conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.sql.session.timeZone": "UTC",
            "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ROOT_USER"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_ROOT_PASSWORD"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        },
        verbose=True,
    )

    raw_path = extract_to_s3(RAW_KEY)
    [raw_path, create_table] >> transform_load


weather_pipeline()

