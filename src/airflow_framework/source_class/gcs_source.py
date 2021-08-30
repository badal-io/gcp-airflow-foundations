import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

from airflow_framework.source_class.source import DagBuilder

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeType
from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import BigQueryMergeTableOperator
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator

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
        landing_dataset = data_source.landing_zone_options["dataset_tmp_name"]

        dags = []
        for table_config in config.tables:
            table_default_task_args = self.default_task_args_for_table(
                config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            dag = DAG(
                dag_id=f"gcs_to_bq_{table_config.table_name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            )

            #1 Load CSV to BQ Landing Zone 
            destination_table = f"{landing_dataset}.{table_config.temp_table_name}"

            load_to_bq_landing = GCSToBigQueryOperator(
                task_id='import_csv_to_bq_landing',
                bucket=gcs_bucket,
                source_objects=gcs_objects,
                destination_project_dataset_table=destination_table,
                schema_object =table_config.temp_schema_object,
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=1,
                dag=dag)

            #2 Check if ODS table exists and if not create it using the provided schema file
            check_table = BigQueryCreateTableOperator(
                task_id='check_table',
                project_id=data_source.gcp_project,
                table_id=table_config.ods_table_name_override,
                dataset_id=data_source.dataset_data_name,
                gcs_schema_object=table_config.ods_schema_object_uri,
                dag=dag
            )

            #3 Merge tables based on surrogate keys and insert metadata columns
            insert_delta_into_ods = BigQueryMergeTableOperator(
                task_id="insert_delta_into_ods",
                project_id=data_source.gcp_project,
                stg_dataset_name=landing_dataset,
                data_dataset_name=data_source.dataset_data_name,
                stg_table_name=table_config.temp_table_name,
                data_table_name=table_config.ods_table_name_override,
                surrogate_keys=table_config.surrogate_keys,
                update_columns=table_config.update_columns,
                merge_type=MergeType.SG_KEY_WITH_HASH,
                column_mapping=table_config.column_mapping,
                dag=dag)
            
            load_to_bq_landing >> check_table >> insert_delta_into_ods

            logging.info(f"Created dag for {table_config}, {dag}")

            dags.append(dag)

        return dags                                  