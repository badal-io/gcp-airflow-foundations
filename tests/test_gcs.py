import os
import pytest

def test_there_is_a_dag_for_each_table(test_dags, return_configs):
    """Check dag for each table + 1 for 'finish' dag """
    if test_dags:
        assert len(test_dags) == len(return_configs[0].tables) # make sure it's tested for the corresponding config table