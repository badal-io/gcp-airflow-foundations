import json
import logging

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def read_schema_from_bq(
    project_id,
    dataset_id,
    table_id,
    google_cloud_storage_conn_id="google_cloud_default",
    gcp_conn_id="google_cloud_default",
    **kwargs
) -> list:
    """
        Helper method to load table schema from the staging table
    """

    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, delegate_to=None)

    schema = bq_hook.get_schema(dataset_id=dataset_id, table_id=table_id, project_id=project_id)

    return schema["fields"]
