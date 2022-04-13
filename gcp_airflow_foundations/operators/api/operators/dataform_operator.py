from airflow.models.baseoperator import BaseOperator
from gcp_airflow_foundations.operators.api.hooks.dataform_hook import DataformHook
from airflow.utils.decorators import apply_defaults
from typing import Any, Optional
from airflow.exceptions import AirflowException
import time
import logging


class DataformOperator(BaseOperator):
    '''
    This operator will use the custom DataformHook to POST to Dataform's ApiService_RunCreate to initiate a new dataform run.
    If no tags are provided, Dataform will run all jobs.
    After a run is created, if the validate_job_submitted parameter is set to True, it will poke Dataform for the run status for a certain amount of time. Use a separate sensor for longer jobs and set validate_job_submitted to False.

    Attributes:
    :param environment: name of the environment, e.g. production, development...
    :type environment: str
    :param schedule: the name of the schedule that will be triggered by airflow
    :type schedule: str
    :param tags: (optional) A list of tags with which the action must have in order to be run. If not set, then all actions will run.
    :type tags: list[str]
    :param submit_mode: (optional) Valid options are 'submit_job_only' (default), 'validate_job_submitted', and 'wait_for_job_to_finish'.
    :type submit_mode: str
    :param retry_period_seconds: (optional) Amount of time to wait for retry if validation job doesn't return SUCCESSFUL. If not set, then default value is 10 seconds.
    :type retry_period_seconds: int

    Instructions to prepare Dataform for the API call:
    1. create schedule in Dataform for REST API call
        a) environments.json >> under the appropriate environment (e.g. prod, dev..) click on "create new schedule"
        b) enter schedule name (e.g. composer)
        c) disable "Enable this schedule"
    2. get Dataform project ID
        easiest way is to look at the url
        E.g. https://app.dataform.co/#/<project_id>/b/<branch>/file/<file-name>
    3. generate API token:
        project settings >> API keys >> GENERATE NEW API KEY

    - Dataform documentation on using REST API: https://docs.dataform.co/dataform-web/api
    - Helpful Medium tutorial: https://medium.com/google-cloud/cloud-composer-apache-airflow-dataform-bigquery-de6e3eaabeb3

    ** Note: Schedules must be on the master branch. In Dataform, you'll have to create a branch first and then merge changes into master.
    '''

    @apply_defaults
    def __init__(
            self,
            *,
            dataform_conn_id: str = 'dataform_default',
            project_id: str,
            environment: str,
            schedule: str,
            tags: Optional[str] = [],
            submit_mode: str = 'submit_job_only',
            retry_period_seconds: Optional[int] = 10,
            **kwargs: Any) -> None:

        super().__init__(**kwargs)
        self.dataform_conn_id = dataform_conn_id
        self.project_id = project_id
        self.environment = environment
        self.schedule = schedule
        self.tags = tags
        self.submit_mode = submit_mode
        self.retry_period_seconds = retry_period_seconds

    def execute(self, context) -> str:
        dataform_hook = DataformHook(dataform_conn_id=self.dataform_conn_id)
        run_url = dataform_hook.run_job(
            project_id=self.project_id,
            environment=self.environment,
            schedule=self.schedule,
            tags=self.tags
        )

        if self.submit_mode == 'submit_job_only':
            return run_url

        while self.submit_mode == 'validate_job_submitted':
            response = dataform_hook.check_job_status(run_url)
            logging.info(f'dataform job status: {response} run rul: {run_url}')

            if response in ('SUCCESSFUL', 'RUNNING'):
                return run_url
            else:
                time.sleep(self.retry_period_seconds)

        while self.submit_mode == 'wait_for_job_to_finish':
            response = dataform_hook.check_job_status(run_url)
            logging.info(f'dataform job status: {response} run rul: {run_url}')

            if response == 'SUCCESSFUL':
                return run_url
            elif response != 'RUNNING':
                raise AirflowException(f"Dataform ApiService Error: {response}")
            else:
                time.sleep(self.retry_period_seconds)
