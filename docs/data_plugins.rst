********************
Data Sources and Sinks
********************

.. sources:
Data Sources
========================

Currently Available Sources
-----------------------------
gcp-airflow-foundations supports ingesting data from the following sources:

- Google Cloud Storage (including loading from Parquet)
- SFTP
- Oracle (using Dataflow)
- MySQL (using Dataflow)
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
gcp-airflow-foundations currently supports ingesting data only to BigQuery.

.. transformation:
Data Transformation
========================
gcp-airflow-foundations is an ingestion framework (EL part of ELT), but it supports triggering commoen transformation  framweworks post ingestion

- `Dataform <https://dataform.co>`_
- `dbt <https://www.getdbt.com/>`_ (work in progress)

Transormations can be scheduled to run using `post ingestion task dependecies <https://github.com/badal-io/gcp-airflow-foundations/blob/main/docs/features.rst#2-post-ingestion-task-dependencies>`_




