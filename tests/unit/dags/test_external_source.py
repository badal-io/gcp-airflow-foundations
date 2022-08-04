import os
import pytest
import unittest

import airflow

from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.parse_dags import DagParser

from tests.unit.dags.external_source import CustomDagBuilder

from airflow.exceptions import AirflowException


class TestExternalSource(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config", "custom", "valid")

    def test_external_source(self):
        assert CustomDagBuilder in DagBuilder.sources

    def test_dag_parser(self):
        parser = DagParser()
        parser.conf_location = self.conf_location
        dags = parser.parse_dags()

        assert isinstance(dags, dict)
        assert all(isinstance(dag, airflow.DAG) for dag in dags.values())
        assert len(dags) == 1


class TestInvalidExternalSource(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config", "custom", "invalid")

    def test_dag_parser(self):
        with pytest.raises(AirflowException) as ctx:
            parser = DagParser()
            parser.conf_location = self.conf_location
            parser.parse_dags()
        assert (
            str(ctx.value) == 'Source "INVALID_SOURCE" is not found in DagBuilder Class'
        )
