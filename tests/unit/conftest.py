import os
import pytest

from airflow import DAG
from airflow.models import DagBag

from airflow_framework.parse_dags import DagParser
from airflow_framework.base_class.utils import load_tables_config_from_dir

import logging

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