import pytest
import logging
import uuid
from datetime import datetime
import pandas as pd
from time import sleep

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow_framework.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema

from tests.integration.conftest import run_task

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


class TestSchemaMigration(object):
    """ 
        End-to-end testing for schema migration for several user cases:
        
            1) Field addition
            2) Field removal
            3) Field type change (valid)
            4) Field type change (invalid)
        
        The audit table is also tested to ensure the change log rows are inserted successfully

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
        
    def insert_mock_data(self, data_rows, schema_fields):
        schema_fields = [SchemaField.from_api_repr(i) for i in schema_fields]

        table = bigquery.Table(f"{self.project_id}.{self.target_dataset}.{self.target_table_id}", schema=schema_fields)
        self.client.create_table(table)

        df = pd.DataFrame.from_dict(data_rows)

        load_job = self.client.load_table_from_dataframe(df, self.target_table_ref)

        while load_job.running():
            sleep(1)

    def migrate_schema(self, new_schema_fields):

        migrate_schema = MigrateSchema(
            task_id=f"{uuid.uuid4().hex}",
            project_id=self.project_id,
            table_id=self.target_table_id,
            dataset_id=self.target_dataset, 
            new_schema_fields=new_schema_fields,
            dag=self.test_dag
        )

        run_task(migrate_schema)
        table = self.client.get_table(f"{self.project_id}.{self.target_dataset}.{self.target_table_id}")

        schema_fields_after_migration = [i.to_api_repr() for i in table.schema]

        return schema_fields_after_migration

    def test_add_field(self):

        schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        self.insert_mock_data(self.mock_data_rows, schema_fields)

        new_mock_schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"STRING", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"},
            {"name":"test_new_column", "type":"STRING", "mode":"NULLABLE"}
        ]

        schema_fields_after_migration = self.migrate_schema(new_mock_schema_fields)

        assert schema_fields_after_migration == new_mock_schema_fields

        self.clean_up()

    def test_change_field_type(self):

        schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        self.insert_mock_data(self.mock_data_rows, schema_fields)

        new_mock_schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"STRING", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        schema_fields_after_migration = self.migrate_schema(new_mock_schema_fields)

        assert schema_fields_after_migration == new_mock_schema_fields

        self.clean_up()

    def test_remove_field(self):

        schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"REQUIRED"}
        ]

        self.insert_mock_data(self.mock_data_rows, schema_fields)

        new_mock_schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        schema_fields_after_migration = self.migrate_schema(new_mock_schema_fields[0:1])

        assert schema_fields_after_migration == new_mock_schema_fields

        self.clean_up()

    def test_change_field_to_invalid_type(self):

        schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        self.insert_mock_data(self.mock_data_rows, schema_fields)

        new_mock_schema_fields = [
            {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
            {"name":"key_id", "type":"TIMESTAMP", "mode":"NULLABLE"},
            {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
        ]

        with pytest.raises(AssertionError) as e:
            schema_fields_after_migration = self.migrate_schema(new_mock_schema_fields)

        assert "Data type of column key_id cannot be changed from INTEGER to TIMESTAMP" in str(e.value)

        self.clean_up()

    def test_audit_table(self):
        sql = f""" SELECT table_id, dataset_id, column_name, type_of_change FROM {self.project_id}.{self.target_dataset}.schema_migration_audit_table """
        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        query_results = self.client.query(sql, job_config=query_config).to_dataframe().to_dict(orient='record')

        audit_rows = [
            {
                "table_id": "test_table",
                "dataset_id": "airflow_test",
                "column_name": "key_id",
                "type_of_change": "data type change"
            },
            {
                "table_id": "test_table",
                "dataset_id": "airflow_test",
                "column_name": "test_new_column",
                "type_of_change": "column addition"
            },
            {
                "table_id": "test_table",
                "dataset_id": "airflow_test",
                "column_name": "city_name",
                "type_of_change": "column mode relaxation"
            }
        ]

        assert all(i in query_results for i in audit_rows)

    def clean_up(self):
        table_ref = self.client.dataset(self.target_dataset).table(self.target_table_id)
        self.client.delete_table(table_ref)
