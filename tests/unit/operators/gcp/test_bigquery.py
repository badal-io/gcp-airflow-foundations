import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.operators.dummy import DummyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime
from unittest import mock

from gcp_airflow_foundations.operators.gcp.create_table import (
    CustomBigQueryCreateEmptyTableOperator
)
from gcp_airflow_foundations.operators.gcp.delete_staging_table import (
    BigQueryDeleteStagingTableOperator
)

from gcp_airflow_foundations.operators.gcp.create_dataset import (
    CustomBigQueryCreateEmptyDatasetOperator
)

TASK_ID = "test-bq-generic-operator"
TEST_DATASET = "test-dataset"
TEST_GCP_PROJECT_ID = "test-project"
TEST_TABLE_ID = "test-table-id"
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = "test-bigquery-operators"
SCHEMA_FIELDS = [{"name": "column", "type": "STRING"}]
TEST_TABLE_RESOURCES = {
    "schema": {"fields": SCHEMA_FIELDS},
    "timePartitioning": None,
    "encryptionConfiguration": None,
    "labels": None,
    # "clustering": {"fields": None}
}


@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestBCustomBigQueryCreateEmptyDatasetOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = DummyOperator(task_id=f"dummy", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook.get_client")
    def test_execute(self, mock_hook):
        operator = CustomBigQueryCreateEmptyDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            location='US',
            exists_ok=True
        )

        operator.execute(context=self.template_context)

        mock_hook.return_value.create_dataset.assert_called_once()


class TestCustomBigQueryCreateEmptyTableOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = DummyOperator(task_id=f"{TEST_TABLE_ID}.schema_parsing", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()
        self.ti.xcom_push(
            key=XCOM_RETURN_KEY,
            value={f"{TEST_DATASET}.{TEST_TABLE_ID}": SCHEMA_FIELDS},
        )

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        operator = CustomBigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            dag_table_id=TEST_TABLE_ID,
        )

        operator.pre_execute(context=self.template_context)

        operator.execute(context=self.template_context)

        mock_hook.return_value.create_empty_table.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=None,
            time_partitioning=None,
            cluster_fields=None,
            labels=None,
            view=None,
            materialized_view=None,
            encryption_configuration=None,
            table_resource=TEST_TABLE_RESOURCES,
            exists_ok=True,
        )


class TestBigQueryDeleteStagingTableOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = DummyOperator(task_id="dummy", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        operator = BigQueryDeleteStagingTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
        )

        operator.pre_execute(context=self.template_context)

        operator.execute(context=self.template_context)

        ds = self.template_context["ds"]

        mock_hook.return_value.delete_table.assert_called_once_with(
            table_id=f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}_{ds}",
            not_found_ok=True,
        )
