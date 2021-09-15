import pytest

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryCreateEmptyTableOperator
)

from pytest_testconfig import config

from tests.integration.conftest import run_task


def test_create_table(test_dag, test_configs):
    new_table = BigQueryCreateEmptyTableOperator(
        task_id="create_mock_table",
        project_id=test_configs.source.gcp_project,
        dataset_id=test_configs.source.landing_zone_options["dataset_tmp_name"],
        table_id='airflow_framework_test_table',
        dag=test_dag
    )
    run_task(new_table)


def test_delete_table(test_dag, test_configs):
    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_mock_table",
        deletion_dataset_table=f"{test_configs.source.gcp_project}.{test_configs.source.landing_zone_options['dataset_tmp_name']}.airflow_framework_test_table",
        ignore_if_missing=False,
        dag=test_dag
    )
    run_task(delete_table)