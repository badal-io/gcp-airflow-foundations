import os
import pytest
import json

from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG

from airflow_framework.base_class.utils import load_tables_config

from airflow.hooks.base_hook import BaseHook

from google.cloud import bigquery


@pytest.fixture(scope="session")
def project_id():
    connection = BaseHook.get_connection("google-cloud-default")
    extra = json.loads(connection.extra)
    return extra["extra__google_cloud_platform__project"]

@pytest.fixture(scope="session")
def test_dag():
    return DAG(
                'test_dag', 
                default_args={'owner': 'airflow', 'start_date': datetime(2021, 1, 1)}
            ) 

def run_task(task):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.execute(ti.get_template_context())

def run_task_with_pre_execute(task):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.pre_execute(ti.get_template_context())
    task.execute(ti.get_template_context())

@pytest.fixture(scope="session")
def config():
    here = os.path.abspath(os.path.dirname(__file__))

    path_parent = os.path.dirname(here)

    conf_location = os.path.join(path_parent, "config/gcs_customer_data.yaml")

    config = load_tables_config(conf_location)

    return config

@pytest.fixture(scope="session")
def staging_dataset(config):
    return config.source.landing_zone_options.landing_zone_dataset

@pytest.fixture(scope="session")
def target_dataset(config):
    return config.source.dataset_data_name

@pytest.fixture(scope="session")
def target_table_id():
    return "test_table"

@pytest.fixture(scope="session")
def mock_data_rows():
    rows = [
        {
            "customerID": "customer_1",
            "key_id": 1,
            "city_name": "Toronto"
        },
        {
            "customerID": "customer_2",
            "key_id": 2,
            "city_name": "Montreal"
        },
        {
            "customerID": "customer_3",
            "key_id": 3,
            "city_name": "Ottawa"
        },
    ]

    return rows
