import logging
import unittest
from datetime import timedelta
import time
import pytest

from airflow import exceptions
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator

from airflow.models import (
    DagBag, 
    DagRun, 
    DagTag,
    TaskInstance,
    DagModel
)
from airflow.models.dag import DAG

from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.session import create_session

from gcp_airflow_foundations.parse_dags import DagParser
from tests.unit.conftest import execute_task

from airflow.models.serialized_dag import SerializedDagModel
from airflow.settings import Session

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DEV_NULL = '/dev/null'

def print_hello():
 return 'Hello Wolrd'

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestUnit(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args, schedule_interval='@once')

    def test_unit(self):
        clear_db_dags()

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=self.dag)

        hello_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        execute_task(task=hello_operator, execution_date=DEFAULT_DATE)

        session = Session()

        r = session.query(DagModel).all()

        assert len(r) == 1
        assert r[0].dag_id == 'TestSource1.TestTable1'

    def test_dataset(self):
        bq_hook = BigQueryHook()

        bq_hook.create_empty_dataset(
            dataset_id='test',
            project_id='airflow-framework'
        )