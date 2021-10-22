import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook

def read_schema_from_bq(
    staging_dataset_id,
    staging_table_id,
    google_cloud_storage_conn_id='google_cloud_default',
    bigquery_conn_id='google_cloud_default',
    **kwargs) -> list:
    """
        Helper method to load table schema from the staging table
    """

    bq_hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, delegate_to=None)

    schema = bq_hook.get_schema(
        dataset_id=staging_dataset_id,
        table_id=staging_table_id
    )

    return schema['fields']
