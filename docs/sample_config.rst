*************
Sample Configuration Files
*************

.. _oracle: 
Oracle
------------------

GCP Airflow Foundations supports data warehouse migration from an Oracle database to BigQuery using Dataflow.

For a detailed description and data type of each configuration field, 
please refer to :class:`gcp_airflow_foundations.base_class.dataflow_job_config.DataflowJobConfig`.

.. code-block:: yaml

    source:
        name: CSG
        source_type: ORACLE
        ingest_schedule: "@daily"
        start_date: "2021-01-01"
        extra_options:
            dataflow_job_config:
                system_name: CSG
                region: us-central1
                bq_load_temp_directory: <GCS directory for loading temporary Dataflow files>
                template_path: <GCS path to Dataflow template>
                jdbc_driver_class: oracle.jdbc.driver.OracleDriver
                jdbc_jar_path: <the GCS path to the driver .jar file>
                jdbc_url: <a valid JDBC url for connecting to the database>
                jdbc_user: <the database username>
                jdbc_pass_secret_name: <the database password>
                kms_key_path: <the KMS key path for encrypting/decrypting JDBC credentials>
                sql_casts: {"DATE": "to_char(COLUMN, 'yyyy-mm-dd') as COLUMN"}     
                table_type_casts: {}
                bq_schema_table: ALL_TAB_COLUMNS
                database_owner: <owner of the tables to query (query scope)>
        location: US
        dataset_data_name: oracle
        connection: google_cloud_default
        landing_zone_options:
            landing_zone_dataset: staging_zone
    tables:
        - table_name: oracle_table
            ingestion_type: FULL
            surrogate_keys: []
            hds_config:
            hds_table_type: SNAPSHOT
            hds_table_time_partitioning: DAY 

.. salesforce: 
Salesforce
------------------

For a detailed description and data type of each configuration field, 
please refer to :class:`gcp_airflow_foundations.base_class.salesforce_ingestion_config.SalesforceIngestionConfig`.

.. code-block:: yaml

    source:
        name: salesforce
        source_type: SALESFORCE
        ingest_schedule: "@daily"
        start_date: "2021-01-01"
        extra_options:
            gcs_bucket: data-lake-bucket
        location: US
        dataset_data_name: salesforce
        landing_zone_options:
            landing_zone_dataset: landing_zone
    tables:
        - table_name: Opportunity
            ingestion_type: FULL
            surrogate_keys: []
            hds_config:
            hds_table_type: SNAPSHOT
            hds_table_time_partitioning: DAY 
            extra_options:
                sf_config:
                    ingest_all_columns: False
                    fields_to_omit: []
                    field_names: ["Id", "OwnerId", "Name", "Amount", "StageName"]
                    api_table_name: Opportunity
        - table_name: Account
            ingestion_type: FULL
            surrogate_keys: []
            hds_config:
            hds_table_type: SNAPSHOT
            hds_table_time_partitioning: DAY 
            extra_options:
                sf_config:
                    ingest_all_columns: False
                    fields_to_omit: []
                    field_names: ["Id","Name"]
                    api_table_name: Account