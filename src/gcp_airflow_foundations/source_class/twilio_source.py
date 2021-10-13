import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig

from gcp_airflow_foundations.source_class.source import DagBuilder

from gcp_airflow_foundations.operators.api.operators.twilio_operator import TwilioToBigQueryOperator
from gcp_airflow_foundations.operators.api.schemas.twilio import get_twilio_schema

from gcp_airflow_foundations.common.gcp.load_builder import load_builder

from urllib.parse import urlparse


class TwilioToBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load data from Twilio's API to a BigQuery Table.
    """
    source_type = "TWILIO"

    def build_dags(self):
        data_source = self.config.source
        logging.info(f"Building DAG for Twilio {data_source.name}")
        project_id = data_source.gcp_project

        logging.info(f"Building DAG for Twilio {data_source.name}")

        # bq args
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset

        dags = []
        for table_config in self.config.tables:
            table_default_task_args = self.default_task_args_for_table(
                self.config, table_config
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

                #2 Create task groups for loading data to ODS/HDS tables
                taskgroups = load_builder(
                    project_id=data_source.gcp_project,
                    table_id=table_config.table_name,
                    dataset_id=data_source.dataset_data_name,
                    landing_zone_dataset=landing_dataset,
                    landing_zone_table_name_override=table_config.landing_zone_table_name_override,
                    surrogate_keys=table_config.surrogate_keys,
                    column_mapping=table_config.column_mapping,
                    gcs_schema_object=None,
                    schema_fields=get_twilio_schema(tableId=destination_table),
                    ods_table_config=table_config.ods_config,
                    hds_table_config=table_config.hds_config,
                    preceding_task=load_to_bq_landing,
                    dag=dag
                )

                logging.info(f"Created dag for {table_config}, {dag}")

                dags.append(dag)

        return dags

    def validate_extra_options(self):
        # Example of extra validations to do
        extra_options = self.config.source.extra_options

        # assert bucket and object/s are non-empty
        assert extra_options["gcs_bucket"]
        assert extra_options["twilio_account_sid"]