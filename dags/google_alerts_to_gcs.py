from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import tempfile

ALERTS_FEEDS = {
    "alerta1": "https://www.google.com.br/alerts/feeds/18059209501699883611/10788229185273033499",
    "alerta2": "https://www.google.com.br/alerts/feeds/18059209501699883611/11237096572336620667",
    "alerta3": "https://www.google.com.br/alerts/feeds/18059209501699883611/16198327923881274455",
}

BUCKET_LANDING = "atalake-20/lnd/monitor_investimento"
BUCKET_RAW = "atalake-20/raw/monitor_investimento"


def fetch_and_save_xml(alert_name, feed_url, **context):
    response = requests.get(feed_url)
    response.raise_for_status()
    xml_content = response.content

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_name = f"google_alerts/{alert_name}/{datetime.now().strftime('%Y-%m-%d')}/{alert_name}.xml"

    gcs_hook.upload(
        bucket_name=BUCKET_LANDING,
        object_name=file_name,
        data=xml_content,
        mime_type="application/xml",
    )

    context["ti"].xcom_push(key=f"{alert_name}_xml_path", value=file_name)


def convert_and_save_parquet(alert_name, **context):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    xml_file_path = context["ti"].xcom_pull(key=f"{alert_name}_xml_path")

    # Download XML from GCS
    xml_content = gcs_hook.download(
        bucket_name=BUCKET_LANDING, object_name=xml_file_path
    )

    # XML parsing
    root = ET.fromstring(xml_content)
    data = []
    for entry in root.findall(".//entry"):
        title = entry.findtext("title")
        link = entry.findtext("link")
        published = entry.findtext("published")
        summary = entry.findtext("content")
        data.append(
            {"title": title, "link": link, "published": published, "summary": summary}
        )

    df = pd.DataFrame(data)

    # Salvar como Parquet temporariamente
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name, index=False)

        raw_file_name = xml_file_path.replace(".xml", ".parquet").replace(
            "google_alerts/", "google_alerts_parquet/"
        )

        gcs_hook.upload(
            bucket_name=BUCKET_RAW,
            object_name=raw_file_name,
            filename=tmp.name,
            mime_type="application/octet-stream",
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
