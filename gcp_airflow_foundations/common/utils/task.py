from airflow.models import TaskInstance


def is_first_task_execution(task_id, dag_id):
    ti = TaskInstance(task_id=task_id, dag_id=dag_id)
    return ti.previous_ti() is None
