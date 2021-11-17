import logging
from datetime import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from gcp_airflow_foundations.operators.facebook.operators.facebook_ads_to_gcs import FacebookAdsReportToBqOperator
from gcp_airflow_foundations.source_class.source import DagBuilder

from airflow.sensors.external_task import ExternalTaskSensor


class FacebooktoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load Facebook Ads data to GCS and then to a staging BigQuery Table.
    """
    source_type = "FACEBOOK"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type

    def get_bq_ingestion_task(self, dag, table_config):
        data_source = self.config.source
        
        task_group = TaskGroup(group_id="ingest_facebook_data")

        facebook_options = data_source.facebook_options

        GCP_PROJECT_ID = data_source.gcp_project
        
        FIELDS = facebook_options.fields
        
        PARAMETERS = {
            "level": facebook_options.level.value, 
            "time_increment": facebook_options.time_increment,
            "breakdowns": table_config.breakdowns,
            "action_breakdowns":table_config.action_breakdowns,
            "use_account_attribution_setting": facebook_options.use_account_attribution_setting,
            "use_unified_attribution_setting": facebook_options.use_unified_attribution_setting
        }

        TIME_RANGE = facebook_options.time_range
        
        run_operator = FacebookAdsReportToBqOperator(
            task_id='fetch_facebook_data_to_bq_staging',
            gcp_project=GCP_PROJECT_ID,
            destination_project_dataset_table=f"{data_source.landing_zone_options.landing_zone_dataset}.{table_config.landing_zone_table_name_override}",
            time_range=TIME_RANGE,
            parameters=PARAMETERS,
            fields=FIELDS,
            account_lookup_scope=facebook_options.account_lookup_scope,
            gcp_conn_id="google_cloud_default",
            api_version="v12.0",
            task_group=task_group,
            dag=dag
        )
        
        if data_source.external_dag_id:
            sensor_op = ExternalTaskSensor(
                task_id='wait_for_accounts_ingestion',
                external_dag_id=data_source.external_dag_id,
                mode='reschedule',
                check_existence=True,
                task_group=task_group,
                dag=dag
            )
            sensor_op >> run_operator
        
        else:
            run_operator
            
        return task_group

    def validate_extra_options(self):
        pass