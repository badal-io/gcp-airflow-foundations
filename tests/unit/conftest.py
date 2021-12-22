import os
import pytest
import json

from datetime import datetime

from airflow.hooks.base_hook import BaseHook

from airflow import DAG
from airflow.models import DagBag, TaskInstance
from airflow.operators.dummy import DummyOperator

from gcp_airflow_foundations.parse_dags import DagParser
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.base_class.utils import load_tables_config

import logging

DEFAULT_DATE = datetime(2015, 1, 1)

def execute_task(task, execution_date):
    ti = TaskInstance(task=task, execution_date=execution_date)
    task.execute(ti.get_template_context())

@pytest.fixture(autouse=True)
def create_dummy_task_instance():
    args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
    
    dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

    task = DummyOperator(task_id='dummy', dag=dag)

    return TaskInstance(task=task, execution_date=DEFAULT_DATE)