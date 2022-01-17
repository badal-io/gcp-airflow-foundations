import os
import sys
from datetime import datetime

from airflow.models import TaskInstance

DEFAULT_DATE = datetime(2015, 1, 1)

sys.path.append(os.path.join(os.path.dirname(__file__), '../test_utils'))


def execute_task(task, execution_date):
    ti = TaskInstance(task=task, execution_date=execution_date)
    task.execute(ti.get_template_context())

def run_task(task):
     ti = TaskInstance(task=task, execution_date=datetime.now())
     task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
     task.execute(ti.get_template_context())

def run_task_with_pre_execute(task):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.pre_execute(ti.get_template_context())
    task.execute(ti.get_template_context())


