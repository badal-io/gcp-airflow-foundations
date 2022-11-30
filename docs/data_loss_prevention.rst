********************
Data Loss Prevention
********************

GCP Airflow Foundations provides a blueprint for deploying a secure Data Lake, with abiltiy to discover, classify, and protect your most sensitive data

.. landing_zone: 
1. Landing Zone
------------------
Ingestion from all sources reuses a common pattern of first loading the data into and landing_zone in BigQuery and then upserting the data into the ODS and HDS.
All data in the landing zone is deleted after a configurable period of time (7 days by default). 

Further all intermideiate GCS files are also deleted after the ingestion is finished

.. dlp:
2. DLP integration and column level securiry 
------------------

As part of the ingestion configuration in Airflow, DLP scan can be enabled for each table - it will run the first time the table is ingest + on a configurable schedule (once a month, etc.)
During each scheduled run, Airflow will
 1. Run a `DLP inspection job <https://cloud.google.com/dlp/docs/creating-job-triggers>`_   on the ODS and HDS tables - it scans the table to detect sensitive data based on a pre-configured `template <https://cloud.google.com/dlp/docs/concepts-templates>`_ which is a set of InfoType (pre created rule for detecting common sensitive data)
 2. Read the results of the job, and if sensitive data is detected in a column, it will apply pre-configured `policy tags <https://cloud.google.com/bigquery/docs/column-level-security-intro>`_ on the column
 3. `Policy tag enforcement <https://cloud.google.com/bigquery/docs/column-level-security#enforce_access_control>`_ (column level security) can be enabled for a specific taxonomy (set of tags)

A sample configuration file to enable DLP integration as part of ingestion can be found : `here <https://github.com/badal-io/gcp-airflow-foundations/blob/main/dags/config/gcs_dlp.yaml>`_

.. vpc_service_control:
3. VPC service control
------------------
While outside of the scope of this framework, we recommend deploying Composer as part of a `secure service permienter <https://cloud.google.com/vpc-service-controls/docs/service-perimeters>`_ to mitigate the risk of data exfiltration. 
For more details please see `these instructions <https://cloud.google.com/composer/docs/configuring-vpc-sc>`_ 

  
