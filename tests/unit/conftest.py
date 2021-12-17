import os
import pytest
import json

from datetime import datetime

from airflow.hooks.base_hook import BaseHook

from airflow import DAG
from airflow.models import DagBag, TaskInstance

from gcp_airflow_foundations.parse_dags import DagParser
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.base_class.utils import load_tables_config

import logging

def run_task(task, execution_date):
    ti = TaskInstance(task=task, execution_date=execution_date)
    task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)
    task.execute(ti.get_template_context())

@pytest.fixture(scope="session")
def test_dags():
    here = os.path.abspath(os.path.dirname(__file__))

    path_parent = os.path.dirname(here)

    parser = DagParser()

    parser.conf_location = os.path.join(path_parent, "config")

    return parser.parse_dags()

@pytest.fixture(scope="session")
def test_configs():
    here = os.path.abspath(os.path.dirname(__file__))

    path_parent = os.path.dirname(here)

    conf_location = os.path.join(path_parent, "config")

    configs = load_tables_config_from_dir(conf_location)

    return configs

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