********************
Features
********************

GCP Airflow Foundations offer a suite of additional features that respond to common data ingestion pitfalls.

.. schema_migration: 
1. Schema Migration
------------------
When ingesting from relational database tables and - to a lesser extend - from third party APIs, the source schema might evolve over time.
GCP Airflow Foundations will detect such changes after loading the data in a staging table, and update the destination table's schema accordingly.
Most schema modifications that are currently supported by `BigQuery <https://cloud.google.com/bigquery/docs/manually-changing-schemas>`_ are also supported here, including:

- Changing a column's data type using the `current conversion rules in Standard SQL <https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#comparison_chart>`_.
- Relaxing a column's mode from `REQUIRED` to `NULLABLE`

Furthermore, a table is also created in BigQuery to log all the schema migration operations for auditing purposes. 
The audit table stores information on the table and dataset name, the timestamp of the schema migration, the columns affected, 
and the type of schema change that was performed.

.. post_ingestion:
2. Post-ingestion Task Dependencies
------------------
The data that are ingested are often needed in downstream analytic workflows. These can be orchestrated in the same Airflow instance by 
utilizing :class:`gcp_airflow_foundations.operators.airflow.external_task.TableIngestionSensor`. From your Python module with the DAG that depends
on a table ingestion, you can create a task that waits for the completion of the ingestion. For example:

.. code-block:: python
    
    from gcp_airflow_foundations.operators.airflow.external_task import TableIngestionSensor
    
    EXTERNAL_SOURCE_TABLES = {
        'data_source_X':[r"^ABC.*"],
        'data_source_Y':[r".*"]
    }

    sensor = TableIngestionSensor(
        task_id='table_ingestion_sensor',
        external_source_tables=EXTERNAL_SOURCE_TABLES,
        dag=dag
    )

The ``external_source_tables`` argument of :class:`gcp_airflow_foundations.operators.airflow.external_task.TableIngestionSensor` is a dictionary.
Each key of the dictionary is a data source and the value is a list, whose elements are regex expressions that will be matched
to the tables under that source. For instance, in the example above, the sensor's state will transition to `success` once 1) the tables of `data_source_X`
that start with "ABC" and 2) all tables of `data_source_Y` are ingested. 

.. dataform:
3. SQL Workflows with Dataform
------------------

`Dataform <https://docs.dataform.co/>`_ is a framework used to manage data transformation workflows and SQL stored procedures in BigQuery.
GCP Airflow Foundations provides a Dataform Operator, such that Dataform runs can be orchestrated in Airflow. 

.. dataform_external:
3.1 Invoking Dataform in an External DAG
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Dataform Operator can be used alongside the post-ingestion Operator in your downstream DAG for cases when the data transformation 
is dependent on the table ingestion DAGs. For example:

.. code-block:: python

    from gcp_airflow_foundations.operators.airflow.external_task import TableIngestionSensor
    from gcp_airflow_foundations.operators.api.operators.dataform_operator import DataformOperator

    from airflow.models.dag import DAG

    EXTERNAL_SOURCE_TABLES = {
        'data_source':['table_to_wait_for']
    }

    with DAG(
        dag_id="dataform",
        schedule_interval="@daily"
    ) as dag:

        sensor = TableIngestionSensor(
            task_id='table_ingestion_sensor',
            external_source_tables=EXTERNAL_SOURCE_TABLES,
            dag=dag
        )   

        dataform = DataformOperator(
            task_id='dataform_transformation',
            environment='production',
            schedule='dataform_schedule_name',
            dag=dag
        )

        sensor >> dataform

.. dataflow:
4. Data Processing with Dataflow
------------------

GCP Airflow Framework supports ingesting data to BigQuery from relational databases, including Oracle and MySQL, using Dataflow jobs. Dataflow is used to ingest the data into a landing zone in BigQuery. Then the data in landing zone is ingested into the ODS and HDS using the common functionality of the framework.  :ref:`ods`. 

An example configuration file for migrating Oracle tables to BigQuery using Dataflow can be found here: :ref:`Oracle`.

4.1 Configuring a DataFlow Job
------------------
Several prerequisites are required for the ingestion:

A driver .jar file must be available in a bucket on Cloud Storage. For example, for Oracle ingestion an ojdbc{#}.jar file should be provided
A path to the compiled Dataflow template must be available in a bucket on Cloud Storage. The JdbcToBigQuery.java template and instructions to compile it are provided by GCP at `this repository <https://github.com/GoogleCloudPlatform/DataflowTemplates>`_.
Another folder in Cloud Storage should be reserved for the Dataflow job temporary write directory. 

A simple way to configure this would be to create a dedicated GCS bucket per source ingestion, with subfolders for the template file, driver and temporary storage. The rest of the configuration are specified in the jdbc config class, and include jdbc credentials, dataflow job parameters, etc.

4.2 Schema Auto-Detection
------------------
As an optional first step in the ingestion the Dataflow job queries the relational database metadata table to retrieve all the tables and their schema. The schema is then compared against the target table schema, and proper schema evolution rules are followed for schema migration :ref:`schema_migration`. This will occur if the configuration parameter “ingest_metadata” is set to True. If the configuration parameter “ingest_metadata” is set to False, however, then a BigQuery table with the same content should be created manually, and the configuration parameter “bq_schema_table” should point to it.

4.3 Deployment into subnetworks 
------------------
In many use-cases, Composer is deployed in a network that doesn’t have direct access to the source databases. For security purposes it is desirable to allow access to databases only from specific subnetworks. Instead of having to deploy a separate Composer cluster in each subnetwork, Dataflow jobs are configurable to run from separate subnetworks for each source. The source configuration parameter “subnetwork” should be specified as the full subnetwork name in the form: regions/{region}/subnetworks/{subnetwork}.

4.4 Work In Progress
------------------
- Auto ingestion of all tables in a database based on inclusion and exclusion rules (e.g. based on regular expressions, by schema in Oracle). 
- Dataflow job partitioning of large table ingestions

