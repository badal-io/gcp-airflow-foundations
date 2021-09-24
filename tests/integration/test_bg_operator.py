import pytest

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryCreateEmptyTableOperator
)

from pytest_testconfig import config

from tests.integration.conftest import run_task


def test_create_table(test_dag, test_configs):
    config = test_configs[0]
    new_table = BigQueryCreateEmptyTableOperator(
        task_id="create_mock_table",
        project_id=config.source.gcp_project,
        dataset_id=config.source.landing_zone_options.landing_zone_dataset,
        table_id='airflow_framework_test_table',
        dag=test_dag
    )
    run_task(new_table)


def test_delete_table(test_dag, test_configs):
    config = test_configs[0]
    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_mock_table",
        deletion_dataset_table=f"{config.source.gcp_project}.{config.source.landing_zone_options.landing_zone_dataset}.airflow_framework_test_table",
        ignore_if_missing=False,
        dag=test_dag
    )
    run_task(delete_table)