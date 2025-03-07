from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.monitor_investimento.google_alerts_to_gcs import (
    fetch_and_save_xml,
    convert_and_save_parquet,
    ALERTS_FEEDS,
)

with DAG(
    "google_alerts_to_gcs",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    for alert_name, feed_url in ALERTS_FEEDS.items():
        fetch_xml_task = PythonOperator(
            task_id=f"fetch_xml_{alert_name}",
            python_callable=fetch_and_save_xml,
            op_kwargs={"alert_name": alert_name, "feed_url": feed_url},
        )

        convert_parquet_task = PythonOperator(
            task_id=f"convert_parquet_{alert_name}",
            python_callable=convert_and_save_parquet,
            op_kwargs={"alert_name": alert_name},
        )

        fetch_xml_task >> convert_parquet_task
