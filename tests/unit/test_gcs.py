import os
import pytest

def test_there_is_a_dag_for_each_table(test_dags, test_configs):
    """Check dag for each table + 1 for 'finish' dag """
    num_tables = 0
    for config in test_configs:
        num_tables += len(config.tables)

    if test_dags:
        assert len(test_dags) == num_tables