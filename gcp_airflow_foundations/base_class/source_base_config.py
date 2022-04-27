from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from dacite import from_dict


from pydantic import validator, root_validator

import datetime
import re
from typing import List, Optional

from regex import E

from gcp_airflow_foundations.base_class.ods_metadata_config import (
    OdsTableMetadataConfig,
)
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.template_ingestion import TemplateIngestion

from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.base_class.facebook_table_config import FacebookTableConfig
from gcp_airflow_foundations.base_class.template_ingestion_options_config import TemplateIngestionOptionsConfig
from gcp_airflow_foundations.base_class.column_udf_config import ColumnUDFConfig


@dataclass
class SourceBaseConfig:
    """
    Base table/template configuration data class.
    """

    @validator("start_date")
    def valid_start_date(cls, v):
        if v is not None:
            assert datetime.datetime.strptime(
                v, "%Y-%m-%d"
            ), "The date format for Start Date should be YYYY-MM-DD"
        return v

    @root_validator(pre=True)
    def valid_hds_dataset(cls, values):
        if (values["cluster_fields"] is not None) and (
            values["column_mapping"] is not None
        ):
            values["cluster_fields"] = [
                values["column_mapping"].get(field, field)
                for field in values["cluster_fields"]
            ]
        return values

    @root_validator(pre=True)
    def valid_column_casting(cls, values):
        if values["column_casting"] is not None:
            assert all(
                [
                    key not in values["surrogate_keys"]
                    for key in values["column_casting"]
                ]
            ), "Column casting is available only for non-key columns."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "Column casting is not currently supported for HDS SCD2 tables."
        return values

    @root_validator(pre=True)
    def valid_new_column_udfs(cls, values):
        if values["new_column_udfs"] is not None:
            assert all(
                [from_dict(data_class=ColumnUDFConfig, data=values["new_column_udfs"][col]) for col in values["new_column_udfs"].keys()]
            ), "New column UDFs must only contain 'function' and 'output_type' keys with corresponding values."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "New column UDFs is not currently supported for HDS SCD2 tables."
        return values
