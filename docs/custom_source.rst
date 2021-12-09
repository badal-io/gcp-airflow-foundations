********************
Developing Custom Data Sources
********************

GCP Airflow Foundations can be readily expanded to ingest data from APIs that are not built-in.
This is possible by creating a class that inherits from the abstract class :class:`gcp_airflow_foundations.source_class.source.DagBuilder`
and implements the abstract method ``get_bq_ingestion_task``, which returns the Airflow task that ingests data from the external API
to a BigQuery staging table. You may have to provide your own Airflow Operators for your data source, if one is not available by the Airflow community.

For example, implemented bellow for Google Ads using the custom ``GoogleAdsQueryToBqOperator`` Operator:

.. code-block:: python

    from data_sources.google_ads.operators.ads import GoogleAdsQueryToBqOperator
    from data_sources.google_ads.config.google_ads_config import ResourceType
    from data_sources.google_ads.config.google_ads_config import GoogleAdsTableConfig

    from airflow.models.dag import DAG

    from gcp_airflow_foundations.source_class.source import DagBuilder


    class GoogleAdstoBQDagBuilder(DagBuilder):
        
        source_type = "GOOGLE_ADS"

        def get_bq_ingestion_task(self, dag, table_config):

            data_source = self.config.source

            google_ads_config = GoogleAdsTableConfig(**table_config.extra_options['google_ads'])

            client_ids = data_source.extra_options['manager_accounts']

            query_operator = GoogleAdsQueryToBqOperator(
                task_id="google_ads_to_bq", 
                client_ids=client_ids,
                manager_accounts=data_source.extra_options['manager_accounts'],
                resource_type=google_ads_config.resource_type,
                project_id=data_source.gcp_project, 
                dateset_id=data_source.landing_zone_options.landing_zone_dataset,
                table_id=table_config.landing_zone_table_name_override,
                api_version = "v8",
                dag=dag
            )

            return query_operator