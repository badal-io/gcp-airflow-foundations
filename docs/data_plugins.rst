********************
Data Sources and Sinks
********************

.. sources:
Data Sources
========================

Currently Available Sources
-----------------------------
The gcp-airflow-foundations supports ingesting data from the following sources:

- Google Cloud Storage (including loading from Parquet)
- SFTP
- Oracle
- Salesforce
- Facebook Ads

Sources in the Making
-----------------------------

- Google Ads
- Snapchat
- The Trade Desk
- LinkedIn Marketing
- TikTok
- Twitter
- Amazon DSP
- CM360 & DV360
- Spotify Ads
- Pinterest

.. sinks:
Data Sinks
========================
The gcp-airflow-foundations library currently supports ingesting data only to BigQuery.

.. transformation:
Data Transformation
========================
In addition to data sources and sinks, GCP Airflow Framework also supports ingegration with popular Google Cloud and third-party
data transformation platforms:
- `Dataflow <https://cloud.google.com/dataflow/docs>`_
- `Dataform <https://dataform.co/>`_
- `dbt <https://www.getdbt.com/>`_  (work in progress)