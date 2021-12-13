********************
Quick Start
********************

.. pip:

Installing from PyPI
========================
Install with ``pip install 'gcp-airflow-foundations'``

.. generating_dags

Generating DAGs
========================
In the Airflow's ``dags_folder`` create a new Python module (e.g. ``parse_dags.py``), which would parse the DAGs from the YAML configuration files:
 
.. code-block:: python
    
    
    from gcp_airflow_foundations.parse_dags import DagParser
    
    parser = DagParser()

    parsed_dags = parser.parse_dags()

    if parsed_dags:
        globals().update(parsed_dags)
    
The YAML files are loaded as dictionaries and then converted to data classes using the open-source `dacite <https://github.com/konradhalas/dacite>`_ Python library. 
Each of the data classes used have their own validators to ensure that the parameters selected by the user are valid. 
For instance, an error will be raised if the ingestion schedule and the partition time of a snapshot HDS table are not compatible with each other. 

.. requirements
Prerequisites
========================

Running on Google Cloud
-------------------------
- An active Google Cloud with a Cloud Composer environment. The minimal Airflow version required is 2.0.2.
- Enable Cloud Composer, Cloud Storage, and BigQuery APIs
- Optional step: setup a `CI/CD pipeline <https://github.com/badal-io/airflow2-local-ci-cd>`_ for your Cloud Composer environmental that installs the dependencies from PyPI and syncs your DAGs.

Running with Docker
-------------------------
If you deploy Airflow from a Docker image then you can add GCP Airflow Foundations to the dependencies of your Docker image.

.. airflow_connections
Airflow Connections
========================
Airflow connections are used to store credentials to communicate with external systems, such as APIs of third-party data sources. 
Depending on the data sources you are ingesting from you will need to set up the required connections. 
You can do so either through the Admin menu in the Airflow UI of your Cloud Composer instance, or by using Secret Manager. 
If you opt for the latter, make sure to follow some `additional steps <https://cloud.google.com/composer/docs/secret-manager>`_ that are required.
