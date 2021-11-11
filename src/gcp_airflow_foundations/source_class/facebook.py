import logging
from datetime import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from gcp_airflow_foundations.operators.facebook.operators.facebook_ads_to_gcs import FacebookAdsReportToBqOperator
from gcp_airflow_foundations.source_class.source import DagBuilder


class FacebooktoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load Facebook Ads data to GCS and then to a staging BigQuery Table.
    """
    source_type = "FACEBOOK"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type

    def get_bq_ingestion_task(self, dag, table_config):
        data_source = self.config.source
        facebook_options = data_source.facebook_options

        GCP_PROJECT_ID = data_source.gcp_project
        
        FIELDS = facebook_options.fields
        #PARAMETERS = {'level': facebook_options.level.value, 'date_preset': facebook_options.date_preset.value, 'time_increment': facebook_options.time_increment}
        
        PARAMETERS = {
            'level': facebook_options.level.value, 
            'time_increment': facebook_options.time_increment
        }
        
        ACCOUNTS = facebook_options.accounts

        run_operator = FacebookAdsReportToBqOperator(
            task_id='fetch_facebook_data_to_bq_staging',
            facebook_acc_ids=ACCOUNTS,
            gcp_project=GCP_PROJECT_ID,
            destination_project_dataset_table=f"{data_source.landing_zone_options.landing_zone_dataset}.{table_config.landing_zone_table_name_override}",
            parameters=PARAMETERS,
            fields=FIELDS,
            gcp_conn_id="google_cloud_default",
            api_version="v12.0",
            dag=dag
        )
            
        return run_operator

    def validate_extra_options(self):
        pass