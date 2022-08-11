from gcp_airflow_foundations.operators.api.hooks.dataform_hook import DataformHook
from airflow.sensors.base import BaseSensorOperator
# from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from typing import Any
import logging


class DataformSensor(BaseSensorOperator):
    # @apply_defaults
    def __init__(self, task_ids: str, dataform_conn_id='dataform_conn_id', **kwargs: Any):
        super().__init__(**kwargs)
        self.task_ids = task_ids
        self.hook = DataformHook(dataform_conn_id=dataform_conn_id)

    def poke(self, context):
        run_url = context['ti'].xcom_pull(key='return_value', task_ids=[self.task_ids])
        response = self.hook.check_job_status(run_url[0])

        if response == 'SUCCESSFUL':
            logging.info(f"Dataform run completed: SUCCESSFUL see run logs at {run_url}")
            return True
        elif response == 'RUNNING':
            logging.info(f"Dataform job running. {run_url}")
            return False
        else:
            raise AirflowException(f"Dataform run {response}: see run logs at {run_url}")
