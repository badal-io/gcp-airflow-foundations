from airflow.utils.session import create_session, provide_session

from airflow.models import (
    DAG,
    TaskInstance,
    XCom,
    DagBag,
    DagRun,
    DagTag,
    DagModel
)
import pytz
from airflow.utils.state import State
from datetime import datetime
from airflow.operators.dummy import DummyOperator

import logging
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()

@provide_session
def print_xcom(session=None):
    logging.info(f"XCOM content is : {session.query(XCom).all()}")

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()

def setup_test_dag(self):
    args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
    self.dag = DAG('TEST_DAG_ID_1', default_args=args, schedule_interval='@once')

    self.dag.create_dagrun(
        run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
    )

    task = DummyOperator(task_id='dummy', dag=self.dag)
    self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    self.template_context = self.ti.get_template_context()