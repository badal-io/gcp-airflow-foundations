from airflow.models.baseoperator import BaseOperator
from typing import Any, Optional

from airflow.exceptions import AirflowException

import requests
import time
import json
import logging

class DataformOperator(BaseOperator):
    '''
    This operator will POST to Dataform's ApiService_RunCreate to initiate a new dataform run.
    If no tags are provided, Dataform will run all.
    Once a run is created, it will use ApiService_RunGet to return information about the run every 10 seconds until the status is no longer RUNNING.

    Attributes:
    :param project_id: Dataform's project ID
    :type project_id: str
    :param api_key: Dataform's API key
    :type api_key: str
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

    def __init__(self, *, project_id: str, api_key: str, environment: str, schedule: str, tags: Optional[str] = [], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.api_key = api_key
        self.environment = environment
        self.schedule = schedule
        self.tags = tags

    def execute(self, context) -> str:
        base_url = f'https://api.dataform.co/v1/project/{self.project_id}/run'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        run_create_request = {
            "environmentName": self.environment,
            "scheduleName": self.schedule,
            "runConfig": {
                "tags": self.tags
            }
        }

        # ApiService_RunCreate: Initiates new dataform runs within the project, and returns the ID of any created runs.
        response = requests.post(base_url, data=json.dumps(run_create_request), headers=headers)

        try:
            # ApiService_RunGet: Returns information about a specific run
            run_url = base_url + '/' + response.json()['id']
        except Exception:
            raise AirflowException(f"Dataform ApiService Error: {response.json()}")

        response = requests.get(run_url, headers=headers)

        while response.json()['status'] == 'RUNNING':
            time.sleep(10)
            # retry after 10 seconds
            response = requests.get(run_url, headers=headers)
            logging.info(response.json())

        if response.json()['status'] == 'SUCCESSFUL':
            return f"Dataform run completed: SUCCESSFUL see run logs at {response.json()['runLogUrl']}"
        else:
            raise AirflowException(f"Dataform run {response.json()['status']} for {response.json()['id']}: see run logs at {response.json()['runLogUrl']}")