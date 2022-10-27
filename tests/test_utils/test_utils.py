from airflow.utils.session import create_session, provide_session

from airflow.models import TaskInstance, XCom, DagRun
from airflow.models.dag import DAG, DagTag, DagModel
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
def print_xcom(session=None, dag_id=None):
    if dag_id is None:
        res = session.query(XCom).all()
    else:
        res = session.query(XCom).filter(XCom.dag_id == dag_id).all()
    logging.info(f"XCOM content is : {res}")


@provide_session
def print_dag_runs(session=None):
    logging.info(f"active DAGRuns  is : {session.query(DagRun).all()}")


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


def setup_test_dag(self):
    args = {"owner": "airflow", "start_date": DEFAULT_DATE}
    self.dag = DAG("TEST_DAG_ID_1", default_args=args, schedule_interval="@once")

    self.dag.create_dagrun(
        run_id="test",
        start_date=DEFAULT_DATE,
        execution_date=DEFAULT_DATE,
        state=State.SUCCESS,
    )

    task = DummyOperator(task_id="dummy_task_id", dag=self.dag)
    self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    self.template_context = self.ti.get_template_context()
