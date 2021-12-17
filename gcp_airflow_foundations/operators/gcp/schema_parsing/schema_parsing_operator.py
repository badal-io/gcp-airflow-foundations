from typing import Optional

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow.exceptions import AirflowException

import logging

from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs
from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema


from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig


class ParseSchema(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        schema_config,
        column_mapping=None,
        data_source=None,
        table_config=None,
        **kwargs
    ) -> list:
        super().__init__(**kwargs)

        self.schema_config = schema_config
        self.column_mapping = column_mapping
        self.data_source = data_source
        self.table_config = table_config

        self.ods_table_config = table_config.ods_config
        self.hds_table_config = table_config.hds_config

    def execute(self, context):
        ds = context['ds']

        schema_source_config_class = self.schema_config
        schema_method = schema_source_config_class().schema_method()
        schema_method_arguments = schema_source_config_class().schema_method_arguments(self.data_source, self.table_config, ds=ds)

        source_schema_fields = schema_method(**schema_method_arguments)
        logging.info(f"Parsed schema using: {schema_method}")

        schema_xcom = {'source_table_columns':[field["name"] for field in source_schema_fields]}

        if self.column_mapping:
                for field in source_schema_fields:
                    if field["name"] in self.column_mapping.keys():
                        field["name"] = self.column_mapping[field["name"]]

        if self.ods_table_config:
            schema_xcom[self.ods_table_config.table_id] = parse_ods_schema(
                schema_fields=source_schema_fields,
                ods_metadata=self.ods_table_config.ods_metadata
            )

        if self.hds_table_config:
            schema_xcom[self.hds_table_config.table_id] = parse_hds_schema(
                schema_fields=source_schema_fields,
                hds_metadata=self.hds_table_config.hds_metadata,
                hds_table_type=self.hds_table_config.hds_table_type
            )

        return schema_xcom