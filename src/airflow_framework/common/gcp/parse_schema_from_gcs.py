import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from urllib.parse import urlparse

from airflow_framework.enums.hds_table_type import HdsTableType


def parse_schema(
    gcs_schema_object=None,
    schema_fields=None,
    column_mapping=None,
    ods_metadata=None,
    hds_metadata=None,
    hds_table_type=None,
    google_cloud_storage_conn_id='google_cloud_default',
    bigquery_conn_id='google_cloud_default') -> list:

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

    conn = bq_hook.get_conn()
    cursor = conn.cursor()

    if column_mapping:
        for field in schema_fields:
            field["name"] = column_mapping[field["name"]]

    if ods_metadata is not None:
        extra_fields = [
            {
                "name": ods_metadata.ingestion_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": ods_metadata.primary_key_hash_column_name,
                "type": "STRING"
            },
            {
                "name": ods_metadata.update_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": ods_metadata.hash_column_name,
                "type": "STRING"
            }
        ]
    
    elif hds_metadata is not None and hds_table_type == HdsTableType.SCD2:
        extra_fields = [
            {
                "name": hds_metadata.eff_start_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": hds_metadata.eff_end_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": hds_metadata.hash_column_name,
                "type": "STRING"
            }
        ]

    elif hds_metadata is not None and hds_table_type == HdsTableType.SNAPSHOT:
        extra_fields = [
            {
                "name": hds_metadata.eff_start_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": hds_metadata.partition_time_column_name,
                "type": "TIMESTAMP"
            },
            {
                "name": hds_metadata.hash_column_name,
                "type": "STRING"
            }
        ]

    else: 
        extra_fields = []   

    schema_fields.extend(extra_fields)

    return schema_fields