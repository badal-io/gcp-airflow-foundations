import logging

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.transfers.facebook_ads_to_gcs import FacebookAdsReportToGcsOperator

from gcp_airflow_foundations.operators.gcp.gcs_to_bigquery import CustomGCSToBigQueryOperator
from gcp_airflow_foundations.source_class.source import DagBuilder


class FacebooktoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load Facebook Ads data to GCS and then to a staging BigQuery Table.
    """
    source_type = "FACEBOOK"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type

    def get_bq_ingestion_task(self, dag, table_config):
        taskgroup = TaskGroup(group_id="facebook_to_bq")

        data_source = self.config.source
        facebook_options = data_source.facebook_options

        GCP_PROJECT_ID = data_source.gcp_project
        GCS_BUCKET = data_source.extra_options["gcs_bucket"]
        GCS_OBJECTS = data_source.extra_options["gcs_objects"]

        FIELDS = facebook_options.fields
        PARAMETERS = {'level': facebook_options.level.value, 'date_preset': facebook_options.date_preset.value}

        run_operator = FacebookAdsReportToGcsOperator(
            task_id='fetch_facebook_data_to_gcs',
            bucket_name=GCS_BUCKET,
            parameters=PARAMETERS,
            fields=FIELDS,
            gcp_conn_id="google_cloud_default",
            object_name=GCS_OBJECTS[0],
            api_version="v11.0",
            task_group=taskgroup,
            dag=dag
        )
            
        schema_config = self.get_schema_method_class()

        load_to_bq_landing = CustomGCSToBigQueryOperator(
            task_id='import_csv_to_bq_landing',
            schema_config=schema_config,
            data_source=data_source,
            table_config=table_config,
            bucket=GCS_BUCKET,
            source_objects=GCS_OBJECTS,
            destination_project_dataset_table=f"{data_source.landing_zone_options.landing_zone_dataset}.{table_config.landing_zone_table_name_override}",
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
            task_group=taskgroup,
            dag=dag
        )

        run_operator >> load_to_bq_landing

        return taskgroup

    def validate_extra_options(self):
        pass