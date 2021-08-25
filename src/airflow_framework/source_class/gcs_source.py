import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

from airflow_framework.source_class.source import DagBuilder

class GCStoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """
    def build_dags(self, config: DataSourceTablesConfig):
        logging.info(f"Building DAG for GCS {config.source.name}")

        # gcs args
        gcs_bucket = config.source.extra_options["gcs_bucket"]
        gcs_objects = config.source.extra_options["gcs_objects"]
        # bq args
        bq_dataset = config.source.dataset_data_name

        dags = []
        for table_config in config.tables:
            table_default_task_args = self.default_task_args_for_table(
                config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            dag = DAG(
                dag_id="test_gcs",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            )

            # Load CSV to BQ
            destination_table = f"{bq_dataset}.{table_config.sink_table_name}"

            load_to_bq = GCSToBigQueryOperator(
                task_id='upload_csv_to_bq',
                bucket=gcs_bucket,
                source_objects=gcs_objects,
                destination_project_dataset_table=destination_table,
                schema_fields=[
                    {'name': 'timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'device_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'property_measured', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'units_of_measurement', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'device_version', 'type': 'STRING', 'mode': 'NULLABLE'},
                ],
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=1,
                dag=dag)

            load_to_bq 

            logging.info(f"Created dag for {table_config}, {dag}")

            dags.append(dag)

        return dags                                  