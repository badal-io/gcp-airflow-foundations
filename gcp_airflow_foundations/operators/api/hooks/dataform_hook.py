from airflow.hooks.base import BaseHook
from typing import Optional
from airflow.exceptions import AirflowException

import requests
import json


class DataformHook(BaseHook):
    '''
    Airflow Hook to connect to Dataform's API and run Dataform jobs.
    This hook will POST to Dataform's ApiService_RunCreate to initiate a new dataform run.
    If no tags are provided, Dataform will run all.

    Attributes:
    :param project_id: project_id
    :type project_id: str
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

    def __init__(self, dataform_conn_id='datafrom_default') -> None:
        self.conn = self.get_connection(dataform_conn_id)
        self.api_key = self.conn.password
        self.headers = {'Authorization': f'Bearer {self.api_key}'}

    def run_job(self, project_id: str, environment: str, schedule: str, tags: Optional[str] = []) -> str:

        base_url = f'https://api.dataform.co/v1/project/{project_id}/run'
        # headers = {'Authorization': f'Bearer {self.api_key}'}
        run_create_request = {
            "environmentName": environment,
            "scheduleName": schedule,
            "runConfig": {
                "tags": tags
            }
        }

        # ApiService_RunCreate: Initiates new dataform runs within the project, and returns the ID of any created runs.
        response = requests.post(base_url, data=json.dumps(run_create_request), headers=self.headers)

        try:
            # ApiService_RunGet: Returns information about a specific run
            run_url = base_url + '/' + response.json()['id']
            return run_url
        except Exception:
            raise AirflowException(f"Dataform ApiService Error: {response.json()}")

    def check_job_status(self, run_url: str) -> str:
        response = requests.get(run_url, headers=self.headers)
        return response.json()['status']
