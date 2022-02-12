import os
import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.operators.dummy import DummyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime
from unittest import mock

from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.operators.gcp.schema_parsing.schema_parsing_operator import (
    ParseSchema,
)
from gcp_airflow_foundations.source_class.schema_source_config import (
    BQLandingZoneSchemaSourceConfig,
)

TASK_ID = "test-bq-generic-operator"
TEST_DATASET = "test-dataset"
TEST_GCP_PROJECT_ID = "test-project"
TEST_TABLE_ID = "test-table-id"
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = "test-bigquery-operators"


@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestParseSchema(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

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

    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery.BigQueryHook.get_schema"
    )
    def test_execute(self, mock_hook):
        source_config = self.config.source
        table_config = next((i for i in self.config.tables), None)

        table_config.ods_config.table_id = f"{table_config.table_name}_ODS"
        table_config.hds_config.table_id = f"{table_config.table_name}_HDS"

        operator = ParseSchema(
            task_id=TASK_ID,
            schema_config=BQLandingZoneSchemaSourceConfig,
            column_mapping=None,
            data_source=source_config,
            table_config=table_config,
        )

        return_value = operator.execute(context=self.template_context)

        ds = self.template_context["ds"]

        schema_xcom = {
            f"{source_config.dataset_data_name}.{table_config.ods_config.table_id}": [
                {"name": "af_metadata_inserted_at", "type": "TIMESTAMP"},
                {"name": "af_metadata_updated_at", "type": "TIMESTAMP"},
                {"name": "af_metadata_primary_key_hash", "type": "STRING"},
                {"name": "af_metadata_row_hash", "type": "STRING"},
            ],
            f"{source_config.dataset_hds_override}.{table_config.hds_config.table_id}": [
                {"name": "af_metadata_created_at", "type": "TIMESTAMP"},
                {"name": "af_metadata_expired_at", "type": "TIMESTAMP"},
                {"name": "af_metadata_row_hash", "type": "STRING"},
            ],
            "source_table_columns": [],
        }

        mock_hook.assert_called_once_with(
            dataset_id=TEST_DATASET, table_id=f"{TEST_TABLE_ID}_{ds}"
        )

        assert return_value == schema_xcom
