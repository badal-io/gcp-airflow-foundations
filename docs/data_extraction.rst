********************
Extracting Data
********************
.. overview:
Overview
========================
GCP Airflow Foundations support the dynamic generation of ETL/ELT DAGs from simple, user-provided configuation files written in YAML.
At minimum, the user declares in the configuration file the derised ingestion mode and the type of the data source, along with the required tables to be extracted.
Optionally, additional parameters can be provided, such us metadata naming and column mapping between the source and destination tables, among others.
GCP Airflow Foundations will parse the information declared in the YAML file to generate the building blocks necessary for generating the DAGs for the desired data ingestion.

An example of a simple configuration file to extract marketing data from Facebook:

.. code-block:: yaml
    source:
        name: facebook_campaigns_ingestion
        source_type: FACEBOOK
        ingest_schedule: "@daily"
        start_date: "2021-01-01"
        dataset_data_name: facebook
        landing_zone_options:
            landing_zone_dataset: staging_zone
        schema_options:
            schema_source_type: AUTO 
        facebook_options:
            account_lookup_scope: full
            fields: [
                "account_id",
                "campaign_id", 
                "impressions",
                "spend",
                "reach",
                "clicks"]
            level: campaign
            time_increment: "1"
    tables:
        - table_name: fb_campaign_insights
            surrogate_keys: ['account_id', 'campaign_id','date_start']
            ingestion_type: INCREMENTAL
            facebook_table_config:
                breakdowns: null
                action_breakdowns: ["action_type"]
                column_mapping:
                date_start: date
        - table_name: fb_campaign_insights_platform_placement
            surrogate_keys: ['account_id', 'campaign_id','date_start','publisher_platform','platform_position']
            ingestion_type: INCREMENTAL
            facebook_table_config:
                breakdowns:  ["publisher_platform","platform_position"]
                action_breakdowns: ["action_type"]
                column_mapping:
                date_start: date