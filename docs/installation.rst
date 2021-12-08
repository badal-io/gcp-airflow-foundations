Quick-start
============

.. pip:

Installing from PyPI
--------------------------
Install with ``pip install 'gcp-airflow-foundations'``

.. generating_dags

Generating DAGs
--------------------------
In the Airflow's ``dags_folder`` create a new Python module (e.g. ``parse_dags.py``), which would parse the DAGs from the configuration files:

::
    from gcp_airflow_foundations.parse_dags import DagParser

    parser = DagParser() 

    parsed_dags = parser.parse_dags()

    if parsed_dags:
        globals().update(parsed_dags)