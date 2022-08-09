import os
import pytest
import pytz
import unittest
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.utils.session import create_session, provide_session
from datetime import datetime

from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import (
    MigrateSchema,
)

TASK_ID = "test-bq-generic-operator"
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 8, 1))
TEST_DAG_ID = "test-bigquery-operators"

NEW_SCHEMA_FIELDS_ADDED_COLUMN = [
    {"name": "visitorId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitNumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitStartTime", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "fullVisitorId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "clientId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "channelGrouping", "type": "STRING", "mode": "NULLABLE"},
    {"name": "socialEngagementType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_inserted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_primary_key_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_row_hash", "type": "STRING", "mode": "NULLABLE"},
    {
        "name": "customDimensions",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "index", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
]

NEW_SCHEMA_FIELDS_DATA_TYPE = [
    {"name": "visitorId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitNumber", "type": "STRING", "mode": "NULLABLE"},
    {"name": "visitId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "visitStartTime", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "fullVisitorId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "clientId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "channelGrouping", "type": "STRING", "mode": "NULLABLE"},
    {"name": "socialEngagementType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_inserted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_primary_key_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_row_hash", "type": "STRING", "mode": "NULLABLE"},
]

NEW_SCHEMA_FIELDS_DATA_TYPE_INVALID = [
    {"name": "visitorId", "type": "DATETIME", "mode": "NULLABLE"},
    {"name": "visitNumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visitStartTime", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "fullVisitorId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "clientId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "channelGrouping", "type": "STRING", "mode": "NULLABLE"},
    {"name": "socialEngagementType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_inserted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "af_metadata_primary_key_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "af_metadata_row_hash", "type": "STRING", "mode": "NULLABLE"},
]


@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestMigrateSchemaColumnAddition(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

        self.source_config = self.config.source
        self.table_config = next((i for i in self.config.tables), None)

        self.table_id = f"{self.table_config.table_name}_ODS"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables="airflow-framework.test_tables.ga_sessions_ODS",
            destination_project_dataset_table=f"{self.source_config.gcp_project}.{self.source_config.dataset_data_name}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    def test_execute(self):
        migrate_schema_ods = MigrateSchema(
            task_id="schema_migration",
            project_id=self.source_config.gcp_project,
            table_id=self.table_id,
            dag_table_id=self.table_id,
            dataset_id=self.source_config.dataset_data_name,
            new_schema_fields=NEW_SCHEMA_FIELDS_ADDED_COLUMN,
        )

        migrate_schema_ods.execute({})

        updated_schema = BigQueryHook().get_schema(
            dataset_id=self.source_config.dataset_data_name,
            table_id=self.table_id,
            project_id=self.source_config.gcp_project,
        )

        assert updated_schema["fields"] == NEW_SCHEMA_FIELDS_ADDED_COLUMN


class TestMigrateSchemaDataTypeChangeValid(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

        self.source_config = self.config.source
        self.table_config = next((i for i in self.config.tables), None)

        self.table_id = f"{self.table_config.table_name}_ODS"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables="airflow-framework.test_tables.ga_sessions_ODS",
            destination_project_dataset_table=f"{self.source_config.gcp_project}.{self.source_config.dataset_data_name}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    def test_execute(self):
        migrate_schema_ods = MigrateSchema(
            task_id="schema_migration",
            project_id=self.source_config.gcp_project,
            table_id=self.table_id,
            dag_table_id=self.table_id,
            dataset_id=self.source_config.dataset_data_name,
            new_schema_fields=NEW_SCHEMA_FIELDS_DATA_TYPE,
        )

        migrate_schema_ods.execute({})

        updated_schema = BigQueryHook().get_schema(
            dataset_id=self.source_config.dataset_data_name,
            table_id=self.table_id,
            project_id=self.source_config.gcp_project,
        )

        assert updated_schema["fields"] == NEW_SCHEMA_FIELDS_DATA_TYPE


class TestMigrateSchemaDataTypeChangeInvalid(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

        self.source_config = self.config.source
        self.table_config = next((i for i in self.config.tables), None)

        self.table_id = f"{self.table_config.table_name}_ODS"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables="airflow-framework.test_tables.ga_sessions_ODS",
            destination_project_dataset_table=f"{self.source_config.gcp_project}.{self.source_config.dataset_data_name}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    def test_execute(self):
        migrate_schema_ods = MigrateSchema(
            task_id="schema_migration",
            project_id=self.source_config.gcp_project,
            table_id=self.table_id,
            dag_table_id=self.table_id,
            dataset_id=self.source_config.dataset_data_name,
            new_schema_fields=NEW_SCHEMA_FIELDS_DATA_TYPE_INVALID,
        )

        with pytest.raises(AirflowException) as ctx:
            migrate_schema_ods.execute({})
        assert (
            str(ctx.value)
            == "Data type of column visitorId cannot be changed from INTEGER to DATETIME"
        )
