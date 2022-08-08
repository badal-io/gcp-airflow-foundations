import json
import logging

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from urllib.parse import urlparse


def read_schema_from_gcs(
    gcs_schema_object,
    google_cloud_storage_conn_id="google_cloud_default",
    bigquery_conn_id="google_cloud_default",
    **kwargs
) -> list:
    """
        Helper method to load table schema from a GCS URI
    """

    parsed_url = urlparse(gcs_schema_object)
    gcs_bucket = parsed_url.netloc
    gcs_object = parsed_url.path.lstrip("/")

    gcs_hook = GCSHook(
        gcp_conn_id=google_cloud_storage_conn_id, delegate_to=None
    )

    schema_fields = json.loads(
        gcs_hook.download(gcs_bucket, gcs_object).decode("utf-8")
    )

    return schema_fields
