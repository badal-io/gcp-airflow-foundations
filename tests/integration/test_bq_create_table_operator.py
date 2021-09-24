import pytest
import unittest
from datetime import datetime
import os 

from airflow import DAG
from airflow.models import TaskInstance

from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator

from tests.integration.conftest import run_task

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryCreateEmptyTableOperator
)

DEFAULT_DATE = datetime(2021, 1, 1)

def test_table_creation(test_dag, test_configs):
    for config in test_configs:
        data_source = config.source
        for table_config in config.tables:
            
            task = BigQueryCreateTableOperator(
                task_id=f'check_create_table_{table_config.table_name}',
                project_id=data_source.gcp_project,
                table_id=table_config.table_name,
                dataset_id=data_source.dataset_data_name,
                hds_table_type=table_config.hds_table_type,
                column_mapping=table_config.column_mapping,
                schema_fields=None,
                gcs_schema_object=table_config.source_table_schema_object,
                hds_metadata=table_config.hds_metadata,
                ods_metadata=table_config.ods_metadata,
                dag=test_dag
            )

            run_task(task)
            
def test_table_deletion(test_dag, test_configs):
    for config in test_configs:
        data_source = config.source
        for table_config in config.tables:    
            delete_table = BigQueryDeleteTableOperator(
                task_id=f"delete_mock_table_{table_config.table_name}",
                deletion_dataset_table=f"{config.source.gcp_project}.{config.source.landing_zone_options.landing_zone_dataset}.airflow_framework_test_table",
                ignore_if_missing=True,
                dag=test_dag
            )
            run_task(delete_table)