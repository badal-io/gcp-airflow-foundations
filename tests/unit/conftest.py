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

def validate_linear_task_order(dag, ordered_task_ids, ignore_tasks_ids = []):
    """Make sure that the tasks are ordered properly - works only of linear DAG (i.e each task has only one down/upstream task)"""
    length = len(ordered_task_ids)
    for idx, task_id in enumerate(ordered_task_ids):
        task = dag.get_task(task_id)

        downstream_task_ids = set(map(lambda task: task.task_id, task.downstream_list))
        upstream_task_ids = set(map(lambda task: task.task_id, task.upstream_list))

        downstream_task_ids = list(downstream_task_ids - set(ignore_tasks_ids))
        upstream_task_ids = list(upstream_task_ids - set(ignore_tasks_ids))

        if idx > 0:
            prev_task = dag.get_task(ordered_task_ids[idx - 1])
            assert(upstream_task_ids == [prev_task.task_id])

        if idx < length - 1:
            next_task = dag.get_task(ordered_task_ids[idx + 1])
            assert(downstream_task_ids == [next_task.task_id])

def compare_deps(test_task, upstream_deps, downstream_dps):
    upstream_task_ids = list(map(lambda task: task.task_id, test_task.upstream_list))
    downstream_task_ids = list(
        map(lambda task: task.task_id, test_task.downstream_list)
    )

    for upstream in upstream_deps:
        assert upstream in upstream_task_ids
    for downstream in downstream_dps:
        assert downstream in downstream_task_ids