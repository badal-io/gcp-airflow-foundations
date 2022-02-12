from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator, root_validator

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import (
    OdsTableMetadataConfig,
)
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning


@dataclass
class OdsTableConfig:
    """
    Attributes:
        ods_metadata : See OdsTableMetadataConfig class
        merge_type : Only SG_KEY_WITH_HASH is currently supported
        ods_table_time_partitioning: Partitioning for BigQuery table. One of DAY, HOUR, or MONTH. Must be provided if partition_column_name is not
        partition_column_name: Column to use for partitioning. Must be provided if ods_table_time_partitioning is not
    """

    ods_table_time_partitioning: Optional[TimePartitioning]  # DAY, HOUR, or MONTH
    partition_column_name: Optional[str]
    ods_metadata: OdsTableMetadataConfig = OdsTableMetadataConfig()
    merge_type: str = "SG_KEY_WITH_HASH"
    ods_table_time_partitioning: Optional[
        TimePartitioning
    ] = None  # DAY, HOUR, or MONTH
    partition_column_name: Optional[str] = None

    @root_validator(pre=True)
    def valid_config(cls, values):
        if values["ods_table_time_partitioning"] is not None:
            assert (
                values["partition_column_name"] is not None
            ), "Please select a column for time partitioning."
        return values
