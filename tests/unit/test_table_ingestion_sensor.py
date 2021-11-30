import logging
import unittest
from datetime import timedelta
import time
import pytest

from airflow import exceptions
from airflow.exceptions import AirflowException

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

from gcp_airflow_foundations.operators.airflow.external_task import TableIngestionSensor
from tests.unit.conftest import run_task


DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DEV_NULL = '/dev/null'

SOURCE = 'TestSource1'
REGEX = r'.*Table1$'

EXTERNAL_SOURCE_TABLES = {
    SOURCE:[REGEX]
}

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        

class TestTableIngestionSensor(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args, schedule_interval='@once')

    def test_table_ingestion_sensor(self):
        clear_db_dags()

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        run_task(task=op, execution_date=DEFAULT_DATE)

    def test_table_ingestion_sensor_multiple_tables(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES['TestSource2'] = [r'.*Table2$']

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        other_ingestion_dag = DAG('TestSource2.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag, other_ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        other_ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\'].' in  cm.output
            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource2: [\'TestSource2.TestTable2\'].' in  cm.output

    def test_table_ingestion_sensor_multiple_tables_overlapping(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES['TestSource2'] = [r'^Test.*']

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        other_ingestion_dag = DAG('TestSource2.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag, other_ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        other_ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\'].' in  cm.output
            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource2: [\'TestSource2.TestTable2\'].' in  cm.output

    def test_table_ingestion_sensor_multiple_dags(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES = {
            SOURCE:[r'.*']
        }

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        other_ingestion_dag = DAG('TestSource1.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag, other_ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        other_ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:2 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\', \'TestSource1.TestTable2\'].' \
                or 'INFO:airflow.task.operators:2 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable2\', \'TestSource1.TestTable1\'].' in cm.output

    def test_table_ingestion_sensor_multiple_regex(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES = {
            SOURCE:[r'TestTable1', r'TestTable2']
        }

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        other_ingestion_dag = DAG('TestSource1.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag, other_ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        other_ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:2 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\', \'TestSource1.TestTable2\'].' \
                or 'INFO:airflow.task.operators:2 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable2\', \'TestSource1.TestTable1\'].' in cm.output

    def test_table_ingestion_sensor_exclusion_regex(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES = {
            SOURCE:[r'(.*)([^Table2]$)',]
        }

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        other_ingestion_dag = DAG('TestSource1.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag, other_ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        other_ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\'].' in cm.output

    def test_table_ingestion_sensor_multiple_regex_overlapping(self):
        clear_db_dags()

        EXTERNAL_SOURCE_TABLES = {
            SOURCE:[r'TestTable1', r'.*']
        }

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with self.assertLogs(op.log, level=logging.INFO) as cm:
            run_task(task=op, execution_date=DEFAULT_DATE)

            assert 'INFO:airflow.task.operators:1 dependent DAGs found for source TestSource1: [\'TestSource1.TestTable1\'].' in cm.output

    def test_catch_invalid_regex_error(self):
        clear_db_dags()

        regex = r'*'
        EXTERNAL_SOURCE_TABLES = {'TestSource1' : [regex]}

        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == f'The regex expression \'{regex}\' is invalid.'

    def test_catch_no_dags_error(self):
        clear_db_dags()

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == "No active dags found."

    def test_catch_no_dags_for_source_error(self):
        clear_db_dags()

        ingestion_dag = DAG('TestSource2.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == f'No active dags found for source {SOURCE}.'

    def test_catch_no_matching_dags_error(self):
        clear_db_dags()

        ingestion_dag = DAG('TestSource1.TestTable2', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == f'No active dags found for source {SOURCE} using regex: \"{REGEX}\".'

    def test_catch_incompatible_schedules_error(self):
        clear_db_dags()

        schedule_interval = '@daily'
        ingestion_dag = DAG('TestSource1.TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval=schedule_interval)
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == f'Incompatible schedule intervals with that of the main DAG: @once.'

    def test_catch_delimiter_error(self):
        clear_db_dags()

        ingestion_dag = DAG('TestSource1_TestTable1', default_args=self.args, end_date=DEFAULT_DATE, schedule_interval='@once')
        DAG.bulk_write_to_db([ingestion_dag])

        ingestion_dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        op = TableIngestionSensor(
            task_id='test_external_dag_sensor_check',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            allowed_states=["success"],
            failed_states=["failed"],
            dag=self.dag
        )

        with pytest.raises(AirflowException) as ctx:
            run_task(task=op, execution_date=DEFAULT_DATE)
        assert str(ctx.value) == f'Table ingestion DAG \"{ingestion_dag.dag_id}\" is not using a dot delimiter.'