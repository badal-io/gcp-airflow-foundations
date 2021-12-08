********************
Features
********************

GCP Airflow Foundations offer a suite of additional features that respond to common data ingestion pitfalls.

.. schema_migration:
Schema Migration
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
Post-ingestion Task Dependencies
------------------
The data that are ingested are often needed in downstream analytic workflows. These can be orchestrated in the same Airflow instance by 
utilizing :class:`gcp_airflow_foundations.operators.airflow.external_task.TableIngestionSensor`. From your Python module with the DAG that depends
on a table ingestion, you can create a task that waits for the completion of the ingestion. For example:

.. code-block:: python
    
    from gcp_airflow_foundations.operators.airflow.external_task import TableIngestionSensor
    
    EXTERNAL_SOURCE_TABLES = {
        'data_source':['table_to_wait_for']
    }

    sensor = TableIngestionSensor(
        task_id='table_ingestion_sensor',
        external_source_tables=EXTERNAL_SOURCE_TABLES,
        dag=dag
    )

The ``external_source_tables`` argument of :class:`gcp_airflow_foundations.operators.airflow.external_task.TableIngestionSensor` is a dictionary.
Each key of the dictionary is a data source and the value is a list, whose elements are regex expressions that will be matched
to the tables under that source.