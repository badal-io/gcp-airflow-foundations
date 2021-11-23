from airflow.models.baseoperator import BaseOperator
from dataform_hook import DataformHook
from airflow.utils.decorators import apply_defaults
from typing import Any, Optional

class DataformOperator(BaseOperator):
    '''
    This operator will use the custom DataformHook to POST to Dataform's ApiService_RunCreate to initiate a new dataform run.
    If no tags are provided, Dataform will run all.
    Once a run is created, it will use ApiService_RunGet to return information about the run every 10 seconds until the status is no longer RUNNING.

    Attributes:
    :param environment: name of the environment, e.g. production, development...
    :type environment: str
    :param schedule: the name of the schedule that will be triggered by airflow
    :type schedule: str
    :param tags: (optional) A list of tags with which the action must have in order to be run. If not set, then all actions will run.
    :type tags: list[str]

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
    def __init__(self, *, dataform_conn_id: str = 'dataform_default', environment: str, schedule: str, tags: Optional[str] = [], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.dataform_conn_id = dataform_conn_id
        self.environment = environment
        self.schedule = schedule
        self.tags = tags

    def execute(self, context) -> str:
        dataform_hook = DataformHook(dataform_conn_id=self.dataform_conn_id)
        dataform_hook.run_job(
            environment=self.environment,
            schedule=self.schedule,
            tags=self.tags
        )