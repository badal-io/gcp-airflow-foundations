from pytest_testconfig import config
import os
import sys

from datetime import datetime

from airflow.models import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow import DAG
import pytz

import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '../test_utils'))
DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))


def test_dag():
    return DAG(dag_id="testdag", start_date=datetime.now())

def run_task(task, context=None, dagassert_success=True):
    logging.info(f"run_task {task}")
    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    # TODO: Should instead be calling ti.run, which internally calls all functions bellow,
    #       but it seems to be failing in some cases
    context = ti.get_template_context()
    task.render_template_fields(context)
    task.pre_execute(context)
    result = task.prepare_for_execution().execute(context)
    if task.do_xcom_push and result is not None:
        ti.xcom_push(key=XCOM_RETURN_KEY, value=result)
    return ti

# @pytest.fixture()
# def reset_environment():
#     """
#     Resets env variables.
#     """
#     logging.info("reset_environment")
#     init_env = os.environ.copy()
#     yield
#     changed_env = os.environ
#     for key in changed_env:
#         if key not in init_env:
#             del os.environ[key]
#         else:
#             os.environ[key] = init_env[key]
#
#
# @pytest.fixture()
# def reset_db():
#     """
#     Resets Airflow db.
#     """
#     logging.info("reset_db")
#
#     from airflow.utils import db
#
#     db.resetdb()
#     yield