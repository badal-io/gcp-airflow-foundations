import logging

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

from airflow_framework.source_class.source import DagBuilder

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator

from airflow_framework.plugins.gcp_custom.ods_load import build_create_ods_load_taskgroup
from airflow_framework.plugins.gcp_custom.hds_load import build_create_hds_load_taskgroup

from airflow_framework.enums.hds_table_type import HdsTableType

from urllib.parse import urlparse


class GCStoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """
    def build_dags(self, config: DataSourceTablesConfig):
        data_source = config.source
        logging.info(f"Building DAG for GCS {data_source.name}")


        # gcs args
        gcs_bucket = data_source.extra_options["gcs_bucket"]
        gcs_objects = data_source.extra_options["gcs_objects"]
        # bq args
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset

        dags = []
        for table_config in config.tables:
            table_default_task_args = self.default_task_args_for_table(
                config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            with DAG(
                dag_id=f"gcs_to_bq_{table_config.table_name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            ) as dag:

                #1 Load CSV to BQ Landing Zone 
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

                hds_table_type = table_config.hds_table_type

                if hds_table_type is None:
                    #2 Create ODS table (if it doesn't exist) and merge or replace it with the staging table
                    taskgroup = build_create_ods_load_taskgroup(
                        project_id=data_source.gcp_project,
                        table_id=table_config.table_name,
                        dataset_id=data_source.dataset_data_name,
                        landing_zone_dataset=landing_dataset,
                        landing_zone_table_name_override=table_config.landing_zone_table_name_override,
                        column_mapping=table_config.column_mapping,
                        gcs_schema_object=table_config.source_table_schema_object,
                        schema_fields=None,
                        ods_metadata=table_config.ods_metadata,
                        surrogate_keys=table_config.surrogate_keys,
                        update_columns=table_config.update_columns,
                        merge_type=table_config.merge_type,
                        ingestion_type=table_config.ingestion_type,
                        dag=dag)
                
                else:
                    taskgroup = build_create_hds_load_taskgroup(
                        project_id=data_source.gcp_project,
                        table_id=table_config.table_name,
                        dataset_id=data_source.dataset_data_name,
                        landing_zone_dataset=landing_dataset,
                        landing_zone_table_name_override=table_config.landing_zone_table_name_override,
                        column_mapping=table_config.column_mapping,
                        gcs_schema_object=table_config.source_table_schema_object,
                        schema_fields=None,
                        hds_metadata=table_config.hds_metadata,
                        surrogate_keys=table_config.surrogate_keys,
                        update_columns=table_config.update_columns,
                        hds_table_type=table_config.hds_table_type,
                        dag=dag)
                    
                load_to_bq_landing >> taskgroup

                logging.info(f"Created dag for {table_config}, {dag}")

                dags.append(dag)

        return dags                                  