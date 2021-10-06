import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from urllib.parse import urlparse


def read_schema_from_gcs(
    gcs_schema_object=None,
    schema_fields=None, 
    column_mapping=None,
    google_cloud_storage_conn_id='google_cloud_default',
    bigquery_conn_id='google_cloud_default') -> list:
    """
        Helper method to load table schema from a GCS URI, unless a list of schema fields is provided to be used instead.
    """

    bq_hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, delegate_to=None)

    if not schema_fields and gcs_schema_object:

        parsed_url = urlparse(gcs_schema_object)
        gcs_bucket = parsed_url.netloc
        gcs_object = parsed_url.path.lstrip('/')

        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=google_cloud_storage_conn_id,
            delegate_to=None)
        schema_fields = json.loads(gcs_hook.download(
            gcs_bucket,
            gcs_object).decode("utf-8"))
    else:
        schema_fields = schema_fields

    source_table_columns = [field["name"] for field in schema_fields]

    if column_mapping:
        for field in schema_fields:
            if field["name"] in column_mapping.keys():
                field["name"] = column_mapping[field["name"]]

    return schema_fields, source_table_columns