import os
import pytest
import unittest

import airflow
from airflow import DAG
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle
from airflow.exceptions import AirflowException

from gcp_airflow_foundations.parse_dags import DagParser
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir

import logging


class TestDagParser(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config", "valid")

    def test_load_config(self):
        configs = load_tables_config_from_dir(self.conf_location)
        assert len(configs) == 1

    def test_dag_parser(self):
        parser = DagParser()
        parser.conf_location = self.conf_location
        dags = parser.parse_dags()

        assert isinstance(dags, dict)
        assert all(isinstance(dag, airflow.DAG) for dag in dags.values())
        assert len(dags) == 2


class TestDags(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        conf_location = os.path.join(here, "config", "valid")
        self.configs = load_tables_config_from_dir(conf_location)

        parser = DagParser()
        parser.conf_location = conf_location
        self.dags = parser.parse_dags().values()

    def test_dag_has_tasks(self):
        for dag in self.dags:
            assert len(dag.tasks) > 0

    def test_table_has_dag(self):
        num_tables = 0
        for config in self.configs:
            num_tables += len(config.tables)

        if self.dags:
            assert len(self.dags) == num_tables


class TestInvalidDagParser(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config", "invalid")

    def test_load_config(self):
        with pytest.raises(AirflowException) as ctx:
            configs = load_tables_config_from_dir(self.conf_location)
        assert str(ctx.value) == 'missing value for field "source.location"'
