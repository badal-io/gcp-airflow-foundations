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

from gcp_airflow_foundations.operators.gcp.hds.hds_merge_table_operator import MergeBigQueryHDS

from gcp_airflow_foundations.base_class.hds_metadata_config import HdsTableMetadataConfig
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema
from gcp_airflow_foundations.enums.ingestion_type import IngestionType

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from airflow.models import TaskInstance

class TestHdsMergeSnapshot(object):
    @pytest.fixture(autouse=True)
    def setup(self, test_dag, project_id, staging_dataset, target_dataset, target_table_id, mock_data_rows):
        self.test_dag = test_dag
        self.project_id = project_id
        self.staging_dataset = staging_dataset
        self.target_dataset = target_dataset
        self.target_table_id = target_table_id
        self.mock_data_rows = mock_data_rows
        self.mock_new_data_rows = [
            {
                "customerID": "customer_4",
                "key_id": 4,
                "city_name": "Vancouver"
            }
        ]

        self.client = bigquery.Client(project=project_id)
        self.target_table_ref = self.client.dataset(target_dataset).table(target_table_id)

        self.columns = ["customerID","key_id","city_name"]
        self.surrogate_keys = ["customerID","key_id"]
        self.column_mapping = {i:i for i in self.columns}
        self.ingestion_type = IngestionType.INCREMENTAL
        self.hds_table_config = HdsTableConfig(
            hds_metadata=HdsTableMetadataConfig(
                eff_start_time_column_name='af_metadata_created_at', 
                eff_end_time_column_name='af_metadata_expired_at', 
                hash_column_name='af_metadata_row_hash', 
            ), 
            hds_table_type=HdsTableType.SNAPSHOT, 
            hds_table_time_partitioning=TimePartitioning.DAY
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

    def insert_new_rows(self):
        staging_table_id = f"{self.project_id}.{self.staging_dataset}.{self.target_table_id}"
 
        df = pd.DataFrame.from_dict(self.mock_new_data_rows)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        load_job = self.client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)

        while load_job.running():
                    sleep(1)
                    
    def create_hds(self):
        time_partitioning = {
            "type":self.hds_table_config.hds_table_time_partitioning.value,
            "field":self.hds_table_config.hds_metadata.partition_time_column_name
        }

        hds_schema_fields = parse_hds_schema(
            schema_fields=self.schema_fields,
            hds_metadata=self.hds_table_config.hds_metadata,
            hds_table_type=self.hds_table_config.hds_table_type
        )

        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"{uuid.uuid4().hex}",
            dataset_id=self.target_dataset,
            table_id=f"{self.target_table_id}_HDS_SNAPSHOT",
            schema_fields=hds_schema_fields,
            time_partitioning=time_partitioning,
            exists_ok=True,
            dag=self.test_dag
        )

        run_task(create_table)

    def test_merge_hds_snapshot(self):
        self.insert_mock_data()
        self.create_hds()

        insert = MergeBigQueryHDS(
            task_id=f"{uuid.uuid4().hex}",
            project_id=self.project_id,
            stg_dataset_name=self.staging_dataset,
            data_dataset_name=self.target_dataset,
            stg_table_name=self.target_table_id,
            data_table_name=f"{self.target_table_id}_HDS_SNAPSHOT",
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            columns=self.columns,
            ingestion_type = self.ingestion_type,
            hds_table_config=self.hds_table_config,
            dag=self.test_dag
        )

        run_task_with_pre_execute(insert)

        sql = f""" 
            SELECT 
                customerID, key_id, city_name
            FROM {self.project_id}.{self.target_dataset}.{self.target_table_id}_HDS_SNAPSHOT 
            ORDER BY key_id ASC, city_name ASC"""

        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        query_results = self.client.query(sql, job_config=query_config).to_dataframe().to_dict(orient='record')

        expected_rows = self.mock_data_rows[:]

        assert query_results == expected_rows

    def test_merge_snapshot_hds_with_change(self):
        self.insert_new_rows()

        insert = MergeBigQueryHDS(
            task_id=f"{uuid.uuid4().hex}",
            project_id=self.project_id,
            stg_dataset_name=self.staging_dataset,
            data_dataset_name=self.target_dataset,
            stg_table_name=self.target_table_id,
            data_table_name=f"{self.target_table_id}_HDS_SNAPSHOT",
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            columns=self.columns,
            ingestion_type = self.ingestion_type,
            hds_table_config=self.hds_table_config,
            dag=self.test_dag
        )

        run_task_with_pre_execute(insert)

        sql = f""" 
            SELECT 
                customerID, key_id, city_name
            FROM {self.project_id}.{self.target_dataset}.{self.target_table_id}_HDS_SNAPSHOT 
            ORDER BY key_id ASC, city_name ASC"""

        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        query_results = self.client.query(sql, job_config=query_config).to_dataframe().to_dict(orient='record')

        expected_rows = [
            {
                "customerID": "customer_1",
                "key_id": 1,
                "city_name": "Toronto"
            },
            {
                "customerID": "customer_2",
                "key_id": 2,
                "city_name": "Montreal"
            },
            {
                "customerID": "customer_3",
                "key_id": 3,
                "city_name": "Ottawa"
            },
            {
                "customerID": "customer_4",
                "key_id": 4,
                "city_name": "Vancouver"
            },
        ]

        assert query_results == expected_rows

        self.clean_up()


    def clean_up(self):
        staging_table_ref = self.client.dataset(self.staging_dataset).table(self.target_table_id)
        self.client.delete_table(staging_table_ref)

        target_table_ref = self.client.dataset(self.target_dataset).table(f"{self.target_table_id}_HDS_SNAPSHOT")
        self.client.delete_table(target_table_ref)