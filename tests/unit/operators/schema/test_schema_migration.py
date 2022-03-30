import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.operators.dummy import DummyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime
from unittest import mock

from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_audit import (
    SchemaMigrationAudit,
)
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import (
    MigrateSchema,
)

TASK_ID = "test-bq-generic-operator"
TEST_DATASET = "test-dataset"
TEST_GCP_PROJECT_ID = "test-project"
TEST_TABLE_ID = "test-table-id"
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = "test-bigquery-operators"
SCHEMA_FIELDS = [{"name": "column", "type": "STRING"}]


@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestMigrateSchema(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test1",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = DummyOperator(task_id="schema_parsing", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()
        self.ti.xcom_push(
            key=XCOM_RETURN_KEY,
            value={f"{TEST_DATASET}.{TEST_TABLE_ID}": SCHEMA_FIELDS},
        )

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch(
        "gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_audit.SchemaMigrationAudit.insert_change_log_rows"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery.BigQueryHook.get_schema"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery.BigQueryHook.run_query"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery.BigQueryHook.update_table_schema"
    )
    def test_execute(
        self,
        mock_bq_update_table_schema,
        mock_bq_run_query,
        mock_bq_get_schema,
        mock_insert_change_log_rows,
    ):
        operator = MigrateSchema(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
            new_schema_fields=None,
        )

        operator.execute(context=self.template_context)

        mock_bq_get_schema.assert_called_once_with(
            dataset_id=TEST_DATASET, table_id=TEST_TABLE_ID
        )

        mock_bq_update_table_schema.assert_called_once_with(
            project_id=TEST_GCP_PROJECT_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            schema_fields_updates=SCHEMA_FIELDS,
            include_policy_tags=False,
        )

        mock_insert_change_log_rows.assert_called_once_with(
            [
                {
                    "table_id": "test-table-id",
                    "dataset_id": "test-dataset",
                    "column_name": "column",
                    "type_of_change": "column addition",
                }
            ]
        )


class TestSchemaMigrationAudit(unittest.TestCase):
    def setUp(self):
        pass

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch(
        "gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_audit.SchemaMigrationAudit.insert_change_log_rows"
    )
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery.BigQueryHook.create_empty_table"
    )
    def test_execute(
        self, mock_create_empty_table, mock_bq_client, mock_insert_change_log_rows
    ):
        migration_audit = SchemaMigrationAudit(
            project_id=TEST_GCP_PROJECT_ID, dataset_id=TEST_DATASET
        )

        migration_audit.insert_change_log_rows(
            change_log=[
                {
                    "table_id": "test-table-id",
                    "dataset_id": "test-dataset",
                    "column_name": "column",
                    "type_of_change": "column addition",
                }
            ]
        )

        # schema_fields = [
        #     {"name": "table_id", "type": "STRING"},
        #     {"name": "dataset_id", "type": "STRING"},
        #     {"name": "schema_updated_at", "type": "TIMESTAMP"},
        #     {"name": "migration_id", "type": "STRING"},
        #     {"name": "column_name", "type": "STRING"},
        #     {"name": "type_of_change", "type": "STRING"},
        # ]

        # mock_create_empty_table.assert_called_once_with(
        #     project_id=TEST_GCP_PROJECT_ID,
        #     dataset_id=TEST_DATASET,
        #     table_id='schema_migration_audit_table',
        #     schema_fields=schema_fields,
        #     exists_ok=True
        # )

    # mock_insert_change_log_rows.insert_rows.assert_called_once()
