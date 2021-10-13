import logging

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig

from gcp_airflow_foundations.source_class.source import DagBuilder

from gcp_airflow_foundations.common.gcp.load_builder import load_builder

from urllib.parse import urlparse


class GCStoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """
    source_type = "GCS"

    def build_dags(self):
        data_source = self.config.source
        logging.info(f"Building DAG for GCS {data_source.name}")

        # gcs args
        gcs_bucket = data_source.extra_options["gcs_bucket"]
        gcs_objects = data_source.extra_options["gcs_objects"]
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
                dag_id=f"gcs_to_bq_{data_source.name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            ) as dag:

                # 1 Load CSV to BQ Landing Zone
                destination_table = f"{landing_dataset}.{table_config.landing_zone_table_name_override}"

                parsed_url = urlparse(table_config.source_table_schema_object)
                gcs_bucket = parsed_url.netloc
                gcs_object = parsed_url.path.lstrip('/')

                load_to_bq_landing = GCSToBigQueryOperator(
                    task_id='import_csv_to_bq_landing',
                    bucket=gcs_bucket,
                    source_objects=gcs_objects,
                    destination_project_dataset_table=destination_table,
                    schema_object=gcs_object,
                    write_disposition='WRITE_TRUNCATE',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=1,
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
                    gcs_schema_object=table_config.source_table_schema_object,
                    schema_fields=None,
                    ods_table_config=table_config.ods_config,
                    hds_table_config=table_config.hds_config,
                    preceding_task=load_to_bq_landing,
                    dag=dag
                )

                dags.append(dag)

        return dags

    def validate_extra_options(self):
        # Example of extra validation to do
        extra_options = self.config.source.extra_options

        # assert bucket and object/s are non-empty
        assert extra_options["gcs_bucket"]
        assert extra_options["gcs_objects"]