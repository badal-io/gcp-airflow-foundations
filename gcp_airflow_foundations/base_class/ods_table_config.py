from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator, root_validator

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning


@dataclass
class OdsTableConfig:
    """
    Attributes:
        merge_type : Only SG_KEY_WITH_HASH is currently supported
        ods_metadata : See OdsTableMetadataConfig class 
        ods_table_time_partitioning: Partitioning for BigQuery table. One of DAY, HOUR, or MONTH
        partition_column_name: Column to use for partitioning
    """
    ods_metadata: OdsTableMetadataConfig
    ods_table_time_partitioning: Optional[TimePartitioning] # DAY, HOUR, or MONTH
    partition_column_name: Optional[str]
    merge_type: str = "SG_KEY_WITH_HASH"

    @root_validator(pre=True)
    def valid_config(cls, values):
        if values['ods_table_time_partitioning'] is not None:
            assert values['partition_column_name'] is not None, 'Please select a column for time partitioning.'
        return values