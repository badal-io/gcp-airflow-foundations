import unittest
from unittest import mock

from datetime import datetime
import pytz

from airflow.operators.dummy import DummyOperator
from airflow.models import (
    DAG,
    TaskInstance,
    XCom,
    DagBag, 
    DagRun, 
    DagTag,
    DagModel
)
from gcp_airflow_foundations.operators.gcp.dlp.dlp_to_datacatalog_taskgroup import dlp_to_datacatalog_builder
from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from test_utils import cleanup_xcom, clear_db_dags, setup_test_dag
from tests.unit.conftest import run_task
from airflow.utils.state import State

TASK_ID = 'test-dlp'
TEST_DATASET = 'test-dataset'
TEST_RESULT_DATASET = 'test-dlp-dataset'
TEST_PROJECT_ID = 'test-project'
TEST_TABLE_ID = 'test-table-id'
TEST_TEMPLATE = "test-template"
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = 'test-dlp-dag'

EXPECTED_TRIGGER = {
    "display_name": f"af_inspect_{TEST_DATASET}.{TEST_TABLE_ID}_with_{TEST_TEMPLATE}",
    "description": f"DLP scan for {TEST_DATASET}.{TEST_TABLE_ID} triggered by Airflow",
    "inspect_job": {
        "storage_config": {
            "big_query_options": {
                "table_reference": {"project_id": TEST_PROJECT_ID, "dataset_id": TEST_DATASET, "table_id": TEST_TABLE_ID},
                "rows_limit_percent": 10,
                "sample_method": "RANDOM_START",
            }
        },
        "inspect_template_name": TEST_TEMPLATE,
        "actions": [{
            "save_findings": {
                "output_config": {
                    "output_schema": "BIG_QUERY_COLUMNS",
                    "table": {"project_id": "test-project", "dataset_id": "test-dlp-dataset", "table_id": "test-table-id_dlp_results"}
                }
            }
        }]
    },
    "triggers": [{"schedule": {"recurrencePeriodDuration":"30d"}}],
    "status": "HEALTHY"
}



class TestDlpSubTask(unittest.TestCase):
    def setUp(self):
        setup_test_dag(self)

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch('airflow.providers.google.cloud.sensors.bigquery.BigQueryHook')
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    @mock.patch('airflow.providers.google.cloud.operators.dlp.CloudDLPHook')
    def test_dlp_args(self, mock_dlp_hook, mock_bq_hook, mock_bq_sensor_hook):
        dlp_source_config = DlpSourceConfig(
            results_dataset_id=TEST_RESULT_DATASET,
            template_name=TEST_TEMPLATE,
            rows_limit_percent=10
        )
        dlp_table_config = DlpTableConfig()

        dlp_taskgroup = dlp_to_datacatalog_builder(
            project_id=TEST_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            dataset_id = TEST_DATASET,
            source_dlp_config = dlp_source_config,
            table_dlp_config = dlp_table_config,
            dag = self.dag
        )


        assert dlp_taskgroup.group_id == "dlp_scan_table"
        assert dlp_taskgroup.children.keys() == {'dlp_scan_table.delete_old_dlp_results', 'dlp_scan_table.scan_table', 'dlp_scan_table.wait_for_dlp_results', 'dlp_scan_table.read_dlp_results'}

        delete_old_dlp_results_task = dlp_taskgroup.children["dlp_scan_table.delete_old_dlp_results"]
        scan_table_task = dlp_taskgroup.children["dlp_scan_table.scan_table"]
        wait_for_dlp_results_task = dlp_taskgroup.children["dlp_scan_table.wait_for_dlp_results"]
        read_dlp_results_task = dlp_taskgroup.children["dlp_scan_table.read_dlp_results"]

        run_task(task=delete_old_dlp_results_task)

        mock_bq_hook.return_value.delete_table.assert_called_once_with(
                table_id=f"{TEST_PROJECT_ID}.{TEST_RESULT_DATASET}.{TEST_TABLE_ID}_dlp_results",
                not_found_ok=True
        )

        run_task(task=scan_table_task)


        mock_dlp_hook.return_value.create_job_trigger.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            job_trigger=EXPECTED_TRIGGER,
            trigger_id=None,
            retry=3,
            timeout=None,
            metadata=None,
        )

        run_task(task=wait_for_dlp_results_task)

        mock_bq_sensor_hook.return_value.table_exists.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_RESULT_DATASET,
            table_id=f"{TEST_TABLE_ID}_dlp_results"
        )



