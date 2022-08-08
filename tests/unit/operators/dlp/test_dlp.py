import unittest
from unittest import mock

from datetime import datetime
import pytz
from airflow.utils import timezone


from gcp_airflow_foundations.operators.gcp.dlp.dlp_to_datacatalog_taskgroup import (
    dlp_to_datacatalog_builder,
    dlp_policy_tag_taskgroup_name,
)
from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from test_utils import cleanup_xcom, clear_db_dags, setup_test_dag
from tests.unit.conftest import run_task
from gcp_airflow_foundations.base_class.dlp_source_config import PolicyTagConfig
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


TASK_ID = "test-dlp"
TEST_DATASET = "test-dataset"
TEST_RESULT_DATASET = "test-dlp-dataset"
TEST_PROJECT_ID = "test-project"
TEST_TABLE_ID = "test-table-id"
TEST_TEMPLATE = "test-template"
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = "test-dlp-dag"
EXPECTED_JOB = {
    "storage_config": {
        "big_query_options": {
            "table_reference": {
                "project_id": "test-project",
                "dataset_id": "test-dataset",
                "table_id": "test-table-id",
            },
            "rows_limit_percent": 10,
            "sample_method": "RANDOM_START",
        }
    },
    "inspect_template_name": "test-template",
    "actions": [
        {
            "save_findings": {
                "output_config": {
                    "output_schema": "BIG_QUERY_COLUMNS",
                    "table": {
                        "project_id": "test-project",
                        "dataset_id": "test-dlp-dataset",
                        "table_id": "test-table-id_dlp_results",
                    },
                }
            }
        }
    ],
}
EXPECTED_TRIGGER = {
    "display_name": f"af_inspect_{TEST_DATASET}.{TEST_TABLE_ID}_with_{TEST_TEMPLATE}",
    "description": f"DLP scan for {TEST_DATASET}.{TEST_TABLE_ID} triggered by Airflow",
    "inspect_job": {
        "storage_config": {
            "big_query_options": {
                "table_reference": {
                    "project_id": TEST_PROJECT_ID,
                    "dataset_id": TEST_DATASET,
                    "table_id": TEST_TABLE_ID,
                },
                "rows_limit_percent": 10,
                "sample_method": "RANDOM_START",
            }
        },
        "inspect_template_name": TEST_TEMPLATE,
        "actions": [
            {
                "save_findings": {
                    "output_config": {
                        "output_schema": "BIG_QUERY_COLUMNS",
                        "table": {
                            "project_id": "test-project",
                            "dataset_id": "test-dlp-dataset",
                            "table_id": "test-table-id_dlp_results",
                        },
                    }
                }
            }
        ],
    },
    "triggers": [{"schedule": {"recurrencePeriodDuration": "30d"}}],
    "status": "HEALTHY",
}


class TestDlpSubTask(unittest.TestCase):
    def setUp(self):
        setup_test_dag(self)

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch(
        "gcp_airflow_foundations.operators.gcp.dlp.get_dlp_bq_inspection_results_operator.BigQueryHook"
    )
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.operators.dlp.CloudDLPHook")
    def test_dlp_args(
        self, mock_dlp_hook, mock_bq_hook, mock_bq_sensor_hook, mock_bq_sensor_dlp
    ):
        table_dlp_config = PolicyTagConfig(
            taxonomy="test", location="us", tag="test_tag"
        )
        dlp_source_config = DlpSourceConfig(
            results_dataset_id=TEST_RESULT_DATASET,
            template_name=TEST_TEMPLATE,
            rows_limit_percent=10,
            policy_tag_config=table_dlp_config,
        )

        dlp_table_config = DlpTableConfig()
        dlp_table_config.set_source_config(dlp_source_config)

        done = DummyOperator(
            task_id="done",
            trigger_rule=TriggerRule.ALL_DONE,
            start_date=timezone.utcnow(),
        )

        taskgroup = TaskGroup(dlp_policy_tag_taskgroup_name(), dag=self.dag)

        dlp_to_datacatalog_builder(
            datastore="ods",
            taskgroup=taskgroup,
            next_task=done,
            project_id=TEST_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            dataset_id=TEST_DATASET,
            table_dlp_config=dlp_table_config,
            dag=self.dag,
        )

        #  assert dlp_taskgroup.group_id == "dlp_policy_tags"
        assert taskgroup.children.keys() == {
            "dlp_policy_tags.delete_old_dlp_results_ods",
            "dlp_policy_tags.scan_table_ods",
            "dlp_policy_tags.read_dlp_results_ods",
            "dlp_policy_tags.update_bq_policy_tags_ods",
        }

        delete_old_dlp_results_task = taskgroup.children[
            "dlp_policy_tags.delete_old_dlp_results_ods"
        ]
        scan_table_task = taskgroup.children["dlp_policy_tags.scan_table_ods"]
        # read_results_task = taskgroup.children["dlp_policy_tags.read_dlp_results_ods"]
        # update_bq_policy_tags_ods = taskgroup.children[
        #     "dlp_policy_tags.update_bq_policy_tags_ods"
        # ]

        #run_task(task=delete_old_dlp_results_task)

        mock_bq_hook.return_value.delete_table.assert_called_once_with(
            table_id=f"{TEST_PROJECT_ID}.{TEST_RESULT_DATASET}.{TEST_TABLE_ID}_dlp_results",
            not_found_ok=True,
        )

        run_task(task=scan_table_task)

        mock_dlp_hook.return_value.create_dlp_job.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            inspect_job=EXPECTED_JOB,
            risk_job=None,
            job_id=None,
            retry=None,
            timeout=None,
            metadata=(),
            wait_until_finished=True,
        )

        # run_task(task=read_results_task)
        #
        # mock_bq_sensor_dlp.return_value.get_records.assert_called()
