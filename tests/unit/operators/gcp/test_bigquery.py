import unittest
from unittest import mock
from unittest.mock import MagicMock

import pytest
from google.cloud.exceptions import Conflict

from datetime import datetime
import pytz

from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
from airflow.models import (
    DAG,
    TaskInstance,
    XCom
)
from airflow.models.xcom import XCOM_RETURN_KEY

from gcp_airflow_foundations.operators.gcp.create_table import CustomBigQueryCreateEmptyTableOperator

TASK_ID = 'test-bq-generic-operator'
TEST_DATASET = 'test-dataset'
TEST_GCP_PROJECT_ID = 'test-project'
TEST_TABLE_ID = 'test-table-id'
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))
TEST_DAG_ID = 'test-bigquery-operators'
SCHEMA_FIELDS = [{'name':'column', 'type':'STRING'}]
TEST_TABLE_RESOURCES = {
    "schema":{'fields': SCHEMA_FIELDS},
    "timePartitioning":None,
    "encryptionConfiguration":None,
    "labels":None
}

from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils import timezone

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


class TestCustomBigQueryCreateEmptyTableOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

        self.dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        task = DummyOperator(task_id='dummy', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()
        self.ti.xcom_push(key=XCOM_RETURN_KEY, value={TEST_TABLE_ID:SCHEMA_FIELDS})

    def doCleanups(self):
        cleanup_xcom()

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = CustomBigQueryCreateEmptyTableOperator(
            task_id=TASK_ID, 
            dataset_id=TEST_DATASET, 
            project_id=TEST_GCP_PROJECT_ID, 
            table_id=TEST_TABLE_ID,
            schema_task_id='dummy'
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
            exists_ok=True
        )