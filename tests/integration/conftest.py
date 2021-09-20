import os
import pytest

from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG

from airflow_framework.base_class.utils import load_tables_config_from_dir

@pytest.fixture(scope="session")
def test_dag():
    return DAG(dag_id="testdag", start_date=datetime.now())


def run_task(task):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.execute(ti.get_template_context())

@pytest.fixture(scope="session")
def test_configs():
    here = os.path.abspath(os.path.dirname(__file__))

    path_parent = os.path.dirname(here)

    conf_location = os.path.join(path_parent, "config")

    configs = load_tables_config_from_dir(conf_location)

    config = configs[0]

    return config