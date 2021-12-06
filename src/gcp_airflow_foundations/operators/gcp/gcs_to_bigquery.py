from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils.decorators import apply_defaults

from gcp_airflow_foundations.source_class.schema_source_config import SchemaSourceConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.base_class.source_config import SourceConfig

import logging


class CustomGCSToBigQueryOperator(GCSToBigQueryOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        schema_config: SchemaSourceConfig,
        data_source: SourceConfig, 
        table_config: SourceTableConfig,
        bucket: str,
        source_objects: str,
        destination_project_dataset_table: str,
        write_disposition: str,
        create_disposition: str,
        skip_leading_rows: int,
        **kwargs,
    ) -> None:
        super(CustomGCSToBigQueryOperator, self).__init__(
            bucket=bucket,
            source_objects=source_objects,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            skip_leading_rows=skip_leading_rows,
            **kwargs,
        )

        self.schema_config = schema_config
        self.data_source = data_source
        self.table_config = table_config

    def pre_execute(self, context) -> None:
        ds = context['ds']

        schema_source_config_class = self.schema_config

        logging.info(f"Parsing schema for staging table using `{schema_source_config_class.__name__}`")

        schema_method = schema_source_config_class().schema_method()
        schema_method_arguments = schema_source_config_class().schema_method_arguments(self.data_source, self.table_config, ds)

        if schema_method and schema_method_arguments:
            self.schema_fields = schema_method(**schema_method_arguments)
        else:
            self.schema_fields = None
            self.autodetect = True