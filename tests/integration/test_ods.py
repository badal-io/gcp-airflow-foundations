import pytest
import logging
import uuid
import hashlib
from datetime import datetime
import pandas as pd
from time import sleep

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator
)

from tests.integration.conftest import run_task, run_task_with_pre_execute

from airflow_framework.operators.gcp.ods.ods_merge_table_operator import MergeBigQueryODS

from airflow_framework.base_class.ods_metadata_config import OdsTableMetadataConfig
from airflow_framework.enums.ingestion_type import IngestionType
from airflow_framework.base_class.ods_table_config import OdsTableConfig
from airflow_framework.common.gcp.ods.schema_utils import parse_ods_schema

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from airflow.models import TaskInstance

class TestOdsMerge(object):
    """ 
        End-to-end testing for ODS table ETL. Steps:
        
            1) Mock data are loaded to staging dataset
            2) MergeBigQueryODS operator is used to insert the staging data to the ODS table
            3) The rows of the ODS table are queried after every insert operation to validate that they match the expected rows

        The tables are cleaned up at the end of the test
    """
    @pytest.fixture(autouse=True)
    def setup(self, test_dag, project_id, staging_dataset, target_dataset, target_table_id, mock_data_rows):
        self.test_dag = test_dag
        self.project_id = project_id
        self.staging_dataset = staging_dataset
        self.target_dataset = target_dataset
        self.target_table_id = target_table_id
        self.mock_data_rows = mock_data_rows

        self.client = bigquery.Client(project=project_id)
        self.target_table_ref = self.client.dataset(target_dataset).table(target_table_id)

        self.columns = ["customerID","key_id","city_name"]
        self.surrogate_keys = ["customerID","key_id"]
        self.column_mapping = {i:i for i in self.columns}
        self.ods_table_config = OdsTableConfig(
            ods_metadata=OdsTableMetadataConfig(
                hash_column_name='af_metadata_row_hash', 
                primary_key_hash_column_name='af_metadata_primary_key_hash', 
                ingestion_time_column_name='af_metadata_inserted_at', 
                update_time_column_name='af_metadata_updated_at'
            ), 
            ingestion_type=IngestionType.INCREMENTAL, 
            merge_type='SG_KEY_WITH_HASH'
        )

        self.schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        self.bq_schema_fields = [SchemaField.from_api_repr(i) for i in self.schema_fields]

    def insert_mock_data(self):
        staging_table_id = f"{self.project_id}.{self.staging_dataset}.{self.target_table_id}"
        table = bigquery.Table(staging_table_id, schema=self.bq_schema_fields)
        self.client.create_table(table)

        df = pd.DataFrame.from_dict(self.mock_data_rows)

        load_job = self.client.load_table_from_dataframe(df, staging_table_id)

        while load_job.running():
            sleep(1)

    def create_ods(self):
        ods_schema_fields, _ = parse_ods_schema(
            gcs_schema_object=None,
            schema_fields=self.schema_fields,
            column_mapping=self.column_mapping,
            ods_metadata=self.ods_table_config.ods_metadata
        )

        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"{uuid.uuid4().hex}",
            dataset_id=self.target_dataset,
            table_id=f"{self.target_table_id}_ODS_INCREMENTAL",
            schema_fields=ods_schema_fields,
            time_partitioning=None,
            exists_ok=True,
            dag=self.test_dag
        )

        run_task(create_table)

    def clean_up(self):
        staging_table_ref = self.client.dataset(self.staging_dataset).table(self.target_table_id)
        self.client.delete_table(staging_table_ref)

        target_table_ref = self.client.dataset(self.target_dataset).table(f"{self.target_table_id}_ODS_INCREMENTAL")
        self.client.delete_table(target_table_ref)

    def test_merge_ods(self):
        self.insert_mock_data()
        self.create_ods()

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
            ods_table_config=self.ods_table_config,
            dag=self.test_dag
        )

        run_task_with_pre_execute(insert)

        sql = f""" 
            SELECT 
                customerID, key_id, city_name
            FROM {self.project_id}.{self.target_dataset}.{self.target_table_id}_ODS_INCREMENTAL 
            ORDER BY key_id ASC"""

        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        query_results = self.client.query(sql, job_config=query_config).to_dataframe().to_dict(orient='record')

        expected_rows = self.mock_data_rows[:]

        assert query_results == expected_rows

        self.clean_up()
