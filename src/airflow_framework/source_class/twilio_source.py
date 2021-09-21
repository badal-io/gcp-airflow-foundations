import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

from airflow_framework.source_class.source import DagBuilder

from airflow_framework.plugins.api.operators.twilio_operator import TwilioToBigQueryOperator
from airflow_framework.plugins.api.schemas.twilio import get_twilio_schema

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator

from airflow_framework.plugins.gcp_custom.load import build_create_load_taskgroup

from urllib.parse import urlparse


class TwilioToBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load data from Twilio's API to a BigQuery Table.
    """
    type = "TWILIO"

    def build_dags(self):
        data_source = self.config.source

        project_id = data_source.gcp_project

        logging.info(f"Building DAG for Twilio {data_source.name}")

        # bq args
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset

        dags = []
        for table_config in self.config.tables:
            table_default_task_args = self.default_task_args_for_table(
                config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            with DAG(
                dag_id=f"twilio_to_bq_{table_config.table_name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            ) as dag:

                destination_table = f"{table_config.landing_zone_table_name_override}"

                # 1 Load Twilio data to BQ Landing Zone
                load_to_bq_landing = TwilioToBigQueryOperator(
                    task_id='import_twilio_to_bq_landing',
                    twilio_account_sid=data_source.extra_options["twilio_account_sid"],
                    project_id=project_id,
                    dataset_id=landing_dataset,
                    schema_fields=get_twilio_schema(tableId=destination_table),
                    table_id=destination_table,
                    dag=dag)

                # 2 Create ODS table (if it doesn't exist) and merge or replace it with the staging table
                taskgroup = build_create_load_taskgroup(
                    project_id=data_source.gcp_project,
                    table_id=table_config.table_name,
                    dataset_id=data_source.dataset_data_name,
                    landing_zone_dataset=landing_dataset,
                    landing_zone_table_name_override=table_config.landing_zone_table_name_override,
                    column_mapping=table_config.column_mapping,
                    gcs_schema_object=None,
                    schema_fields=get_twilio_schema(tableId=destination_table),
                    ods_metadata=table_config.ods_metadata,
                    surrogate_keys=table_config.surrogate_keys,
                    update_columns=table_config.update_columns,
                    merge_type=table_config.merge_type,
                    ingestion_type=table_config.ingestion_type,
                    dag=dag)

                load_to_bq_landing >> taskgroup

                logging.info(f"Created dag for {table_config}, {dag}")

                dags.append(dag)

        return dags
