
import pytest
import uuid
import pandas as pd
from time import sleep

from base_class.dlp_source_config import DlpSourceConfig
from base_class.dlp_table_config import DlpTableConfig
from operators.gcp.dlp.dlp_to_datacatalog_taskgroup import dlp_to_datacatalog_builder
from tests.integration.conftest import run_task, run_task_with_pre_execute

from gcp_airflow_foundations.operators.gcp.ods.ods_merge_table_operator import MergeBigQueryODS

from google.cloud import bigquery

class TestOdsMerge(object):
    """
        End-to-end testing for DLP to Data Catalog . Steps:

            1) Mock data is loaded to staging dataset
            2) MergeBigQueryODS operator is used to insert the staging data to the ODS table
            3) The rows of the ODS table are queried after every insert operation to validate that they match the expected rows

        The tables are cleaned up at the end of the test
    """
    @pytest.fixture(autouse=True)
    def setup(self, test_dag, project_id, staging_dataset, target_dataset, target_table_id, mock_dlp_data_1, mock_dlp_data_bq_schema_1):
        self.test_dag = test_dag
        self.project_id = project_id
        self.staging_dataset = staging_dataset
        self.target_dataset = target_dataset
        self.target_table_id = target_table_id

        self.client = bigquery.Client(project=project_id)
        self.target_table_ref = self.client.dataset(target_dataset).table(target_table_id)

        self.dlp_source_config = DlpSourceConfig(
            results_dataset_id=target_dataset,
            template_name="test1"
        )
        self.dlp_source_config          = DlpTableConfig()
        self.mock_dlp_data              = mock_dlp_data_1
        self.mock_dlp_data_bq_schema   = mock_dlp_data_bq_schema_1


def insert_mock_data(self, data, bq_schema):
        staging_table_id = f"{self.project_id}.{self.staging_dataset}.{self.target_table_id}"

        table = bigquery.Table(staging_table_id, schema=bq_schema)
        self.client.create_table(table)

        df = pd.DataFrame.from_dict(data)



        load_job = self.client.load_table_from_dataframe(df, staging_table_id)

        while load_job.running():
            sleep(1)

    def run_dlp(self):
        dlp_task = dlp_to_datacatalog_builder(
            project_id = self.project_id,
            table_id = self.target_table_id,
            dataset_id = self.staging_dataset,
            rows_limit_percent = 100,
            source_dlp_config = self.dlp_source_config,
            table_dlp_config = self.dlp_source_config,
            dag = self.test_dag
        )

        run_task(dlp_task)

    def clean_up(self):
        staging_table_ref = self.client.dataset(self.staging_dataset).table(self.target_table_id)
        self.client.delete_table(staging_table_ref)

        target_table_ref = self.client.dataset(self.target_dataset).table(f"{self.target_table_id}_ODS_INCREMENTAL")
        self.client.delete_table(target_table_ref)

    def test_dlp(self):
        self.insert_mock_data(self.mock_dlp_data, self.mock_dlp_data_bq_schema)
        self.run_dlp()

        insert = MergeBigQueryODS(
            task_id=f"{uuid.uuid4().hex}",
            project_id=self.project_id,
            stg_dataset_name=self.staging_dataset,
            data_dataset_name=self.target_dataset,
            stg_table_name=self.target_table_id,
            data_table_name=f"{self.target_table_id}_ODS_INCREMENTAL",
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            columns=self.columns,
            ingestion_type=self.ingestion_type,
            ods_table_config=self.ods_table_config,
            dag=self.test_dag
        )

        run_task_with_pre_execute(insert)

        # sql = f"""
        #     SELECT
        #         customerID, key_id, city_name
        #     FROM {self.project_id}.{self.target_dataset}.{self.target_table_id}_ODS_INCREMENTAL
        #     ORDER BY key_id ASC"""
        #
        # query_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        #
        # query_results = self.client.query(sql, job_config=query_config).to_dataframe().to_dict(orient='record')
        #
        # expected_rows = self.mock_data_rows[:]
        #
        # assert query_results == expected_rows

        self.clean_up()



