import logging

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from gcp_airflow_foundations.operators.gcp.gcs_to_bigquery import CustomGCSToBigQueryOperator

from gcp_airflow_foundations.source_class.source import DagBuilder

from gcp_airflow_foundations.enums.schema_source_type import SchemaSourceType


class GCStoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """
    source_type = "GCS"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type

    def get_bq_ingestion_task(self, dag, table_config):
        # 1 Load CSV to BQ Landing Zone with schema auto-detection
        data_source = self.config.source

        schema_config = self.get_schema_method_class()

        load_to_bq_landing = CustomGCSToBigQueryOperator(
            task_id='import_csv_to_bq_landing',
            schema_config=schema_config,
            data_source=data_source,
            table_config=table_config,
            bucket=data_source.extra_options["gcs_bucket"],
            source_objects=data_source.extra_options["gcs_objects"],
            destination_project_dataset_table=f"{data_source.landing_zone_options.landing_zone_dataset}.{table_config.landing_zone_table_name_override}",
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1
        )

        return load_to_bq_landing
             
    def validate_extra_options(self):
        # Example of extra validation to do
        extra_options = self.config.source.extra_options

        # assert bucket and object/s are non-empty
        assert extra_options["gcs_bucket"]
        assert extra_options["gcs_objects"]