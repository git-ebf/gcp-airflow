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

BUCKET = "atalake-20"
BUCKET_LANDING = "/lnd/monitor_investimento"
BUCKET_RAW = "/raw/monitor_investimento"


def fetch_and_save_xml(alert_name, feed_url, **context):
    response = requests.get(feed_url)
    response.raise_for_status()
    xml_content = response.content

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_name = f"{BUCKET_LANDING}/google_alerts/{alert_name}/{datetime.now().strftime('%Y-%m-%d')}/{alert_name}.xml"

    gcs_hook.upload(
        bucket_name=BUCKET,
        object_name=file_name,
        data=xml_content,
        mime_type="application/xml",
    )

    context["ti"].xcom_push(key=f"{alert_name}_xml_path", value=file_name)


def convert_and_save_parquet(alert_name, **context):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    xml_file_path = BUCKET_LANDING + context["ti"].xcom_pull(
        key=f"{alert_name}_xml_path"
    )

    # Download XML from GCS
    xml_content = gcs_hook.download(bucket_name=BUCKET, object_name=xml_file_path)

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

        raw_file_name = BUCKET_RAW + xml_file_path.replace(".xml", ".parquet").replace(
            "google_alerts/", "google_alerts_parquet/"
        )

        gcs_hook.upload(
            bucket_name=BUCKET,
            object_name=raw_file_name,
            filename=tmp.name,
            mime_type="application/octet-stream",
        )
