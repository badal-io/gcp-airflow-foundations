import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

from airflow_framework.source_class.source import DagBuilder

from airflow_framework.plugins.api.operators.twilio_operator import TwilioToBigQueryOperator
from airflow_framework.plugins.api.schemas.twilio import get_twilio_schema
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator


class TwilioToBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load data from Twilio's API to a BigQuery Table.
    """
    def build_dags(self, config: DataSourceTablesConfig):
        data_source = config.source

        project_id = data_source.gcp_project

        logging.info(f"Building DAG for Twilio {data_source.name}")

        # bq args
        landing_dataset = data_source.landing_zone_options["dataset_tmp_name"]

        dags = []
        for table_config in config.tables:
            table_default_task_args = self.default_task_args_for_table(
                config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            dag = DAG(
                dag_id=f"twilio_to_bq_{table_config.table_name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            )

            
            destination_table = f"{table_config.temp_table_name}"

            #1 Check if table already exists in the BQ landing zone and if not create it
            check_table = BigQueryCreateTableOperator(
                task_id='check_table',
                project_id=project_id,
                table_id=destination_table,
                dataset_id=landing_dataset,
                schema_fields=get_twilio_schema(tableId=destination_table),
                gcs_schema_object=table_config.ods_schema_object_uri,
                dag=dag
            )


            #2 Load Twilio data to BQ Landing Zone 
            load_to_bq_landing = TwilioToBigQueryOperator(
                task_id='import_twilio_to_bq_landing',
                project_id=project_id,
                dataset_id=landing_dataset,
                table_id=destination_table,
                dag=dag)

            check_table >> load_to_bq_landing

            logging.info(f"Created dag for {table_config}, {dag}")

            dags.append(dag)

        return dags                                  