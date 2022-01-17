import pytest

from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.operators.gcp.dlp.dlp_to_datacatalog_taskgroup import dlp_to_datacatalog_builder
from tests.integration.conftest import run_task, run_task_with_pre_execute
from airflow.operators.dummy import DummyOperator

from bq_test_utils import insert_to_bq_from_dict
from test_utils import cleanup_xcom, clear_db_dags, setup_test_dag, print_xcom
from pytest_testconfig import config
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator, BigQueryOperator
import pandas as pd
from google.cloud import bigquery

import logging

TEST_TABLE = "test_table"
class TestDlp(object):
    """
        End-to-end testing for DLP to Data Catalog . Steps:

            1) Mock data is loaded to staging dataset
            2) MergeBigQueryODS operator is used to insert the staging data to the ODS table
            3) The rows of the ODS table are queried after every insert operation to validate that they match the expected rows

        The tables are cleaned up at the end of the test
    """

    TEST_SCHEMA = schema_fields = [
        {"name":"customerID", "type":"STRING", "mode":"NULLABLE", "bla": "bla"},
        {"name":"email", "type":"STRING", "mode":"NULLABLE"},
        {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
    ]
    @pytest.fixture(autouse=True)
    def setUp(self):
        cleanup_xcom()
        clear_db_dags()
        setup_test_dag(self)
        self.client = bigquery.Client(project=config["gcp"]["project_id"])

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    def run_dlp(self, dag, project_id, dataset_id, target_table_id):
        template_name="projects/airflow-framework/locations/global/inspectTemplates/test1"
                      # config['dlp']['template']

        dlp_source_config = DlpSourceConfig(
            results_dataset_id=dataset_id,
            template_name=template_name,
            rows_limit_percent=10
        )
        dlp_table_config = DlpTableConfig()

        logging.info(f"project_id {project_id} dataset_id {dataset_id } dlp_source_config {dlp_source_config}")
        dlp_taskgroup = dlp_to_datacatalog_builder(
            project_id=project_id,
            table_id=target_table_id,
            dataset_id =dataset_id,
            source_dlp_config = dlp_source_config,
            table_dlp_config = dlp_table_config,
            dag=dag
        )

        logging.info(f"children keys:{dlp_taskgroup.children.keys()}")
        delete_old_dlp_results_task = dlp_taskgroup.children["dlp_scan_table.delete_old_dlp_results"]
        scan_table_task = dlp_taskgroup.children["dlp_scan_table.scan_table"]
        read_dlp_results_task = dlp_taskgroup.children["dlp_scan_table.read_dlp_results"]
        update_tags_task = dlp_taskgroup.children["dlp_scan_table.update_bq_policy_tags"]

        # run_task(delete_old_dlp_results_task)
        # run_task(scan_table_task)
        t1 = run_task(read_dlp_results_task)
        t1.xcom_push(key="test_key", value="123")
        print_xcom()
        scan_results = t1.xcom_pull(task_ids="dlp_scan_table.read_dlp_results", key="results")
        logging.info(f"scan_results = {scan_results}")

        run_task_with_pre_execute(update_tags_task)

        table = self.client.get_table(f"{project_id}.{dataset_id}.{target_table_id}")

        schema = table.to_api_repr()['schema']['fields']


        assert schema == self.TEST_SCHEMA
        logging.info("done")
        # pd.testing.assert_frame_equal(
        #     pd.DataFrame(scan_results),
        #     pd.DataFrame([['email', 'EMAIL_ADDRESS', 'VERY_LIKELY', 21], ['email', 'DOMAIN_NAME', 'LIKELY', 21], ['city_name', 'LOCATION', 'POSSIBLE', 21]]),
        #     check_index_type=False,
        # )

    def test_dlp(self):
        table_id = "test_table"
        project_id = config["gcp"]["project_id"]
        dataset_id = f"{config['bq']['dataset_prefix']}"
        mock_dlp_data = [
            {
                "customerID": "customer_1",
                "email": "c1@gmail.com",
                "city_name": "Toronto"
            },
            {
                "customerID": "customer_2",
                "email": "c2@gmail.com",
                "city_name": "Montreal"
            },
            {
                "customerID": "customer_3",
                "email": "c3@hotmail.com",
                "city_name": "Ottawa"
            },
        ]
        insert_to_bq_from_dict(mock_dlp_data, project_id, dataset_id, table_id)
        self.run_dlp(self.dag, project_id, dataset_id, table_id)

        self.doCleanups()