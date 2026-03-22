"""
brewery_pipeline_dag.py
-----------------------
Main Airflow DAG for the Brewery Analytics Pipeline.
Runs daily and orchestrates the full pipeline:

  fetch_breweries
       │
  fetch_weather
       │
  spark_transform
       │
  dbt_run
       │
  dbt_test

Each task is independent and can be retried on failure.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Default arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="brewery_analytics_pipeline",
    default_args=default_args,
    description="End-to-end brewery analytics pipeline",
    schedule_interval="0 6 * * *",   # runs daily at 6am UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["brewery", "etl", "analytics"],
) as dag:

    # ── Task 1: Fetch breweries ────────────────────────────────────────────
    def run_fetch_breweries():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.fetch_breweries import main
        main()

    fetch_breweries = PythonOperator(
        task_id="fetch_breweries",
        python_callable=run_fetch_breweries,
    )

    # ── Task 2: Fetch weather ──────────────────────────────────────────────
    def run_fetch_weather():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.fetch_weather import main
        main()

    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=run_fetch_weather,
    )

    # ── Task 3: Upload to S3 ───────────────────────────────────────────────
    def run_upload_to_s3():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.upload_to_s3 import main
        main()

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload_to_s3,
    )

    # ── Task 4: Spark transformation ───────────────────────────────────────
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="spark-submit /opt/airflow/spark/transform_breweries.py",
    )

    # ── Task 5: dbt run ────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/brewery_dbt && dbt run --profiles-dir /opt/airflow/dbt/brewery_dbt",
    )

    # ── Task 6: dbt test ───────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/brewery_dbt && dbt test --profiles-dir /opt/airflow/dbt/brewery_dbt",
    )

    # ── Task dependencies ──────────────────────────────────────────────────
    # This defines the execution order:
    # fetch_breweries → fetch_weather → upload_to_s3 → spark_transform → dbt_run → dbt_test
    fetch_breweries >> fetch_weather >> upload_to_s3 >> spark_transform >> dbt_run >> dbt_test
