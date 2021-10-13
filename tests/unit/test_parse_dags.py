import os
import pytest

from airflow import DAG
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle

from gcp_airflow_foundations.parse_dags import DagParser

import logging

def test_dag_parsing_errors(test_dags):
    assert isinstance(test_dags, dict)

    assert len(test_dags) == 1

    assert all(isinstance(dag, DAG) for dag in test_dags.values())

def test_no_cyclic_dags(test_dags):
    for dag in test_dags.values():
        assert test_cycle(dag) is None

def test_dag_has_tasks(test_dags):
    for dag in test_dags.values():
        assert len(dag.tasks) > 0

def test_there_is_a_dag_for_each_table(test_dags, test_configs):
    """Check dag for each table + 1 for 'finish' dag """
    num_tables = 0
    for config in test_configs:
        num_tables += len(config.tables)

    if test_dags:
        assert len(test_dags) == num_tables