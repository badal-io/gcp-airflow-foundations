import os
import pytest

from airflow import DAG
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle

from airflow_framework.parse_dags import DagParser

import logging

from airflow_framework.plugins.gcp_custom.sql_upsert_helpers import create_truncate_sql, create_upsert_sql_with_hash
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


def get_columns(stg_dataset_name, stg_table_name):
    hook = BigQueryHook(
        bigquery_conn_id="google_cloud_default",
        delegate_to=None,
    )

    conn = hook.get_conn()
    bq_cursor = conn.cursor()

    schema = bq_cursor.get_schema(
        dataset_id=stg_dataset_name, table_id=stg_table_name
    )

    columns: list[str] = list(map(lambda x: x["name"], schema["fields"]))

    return columns

def test_sql_with_hash(test_configs):
    for config in test_configs:
        data_source = config.source
        for table_config in config.tables:
            columns = get_columns(data_source.landing_zone_options.landing_zone_dataset, table_config.table_name)

            sql_with_hash = create_upsert_sql_with_hash(
                data_source.landing_zone_options.landing_zone_dataset,
                data_source.dataset_data_name,
                table_config.table_name,
                table_config.dest_table_override,
                table_config.surrogate_keys,
                columns,
                table_config.column_mapping,
                table_config.ods_metadata
            )

            assert sql_with_hash is not None

def test_sql_truncate(test_configs):
    for config in test_configs:
        data_source = config.source
        for table_config in config.tables:
            columns = get_columns(data_source.landing_zone_options.landing_zone_dataset, table_config.table_name)

            sql_truncate = create_truncate_sql(
                data_source.landing_zone_options.landing_zone_dataset,
                data_source.dataset_data_name,
                table_config.table_name,
                table_config.dest_table_override,
                table_config.surrogate_keys,
                columns,
                table_config.column_mapping,
                table_config.ods_metadata
            )

            assert sql_truncate is not None