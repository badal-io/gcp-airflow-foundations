from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator

from datetime import datetime
from typing import List, Optional

from airflow_framework.base_class.hds_metadata_config import HdsTableMetadataConfig
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.enums.time_partitioning import TimePartitioning

@dataclass
class HdsTableConfig:
    """
    Attributes:
        hds_table_type : SNAPSHOT OR SCD2
        hds_table_time_partitioning: Partitioning for BigQuery table. One of DAY, HOUR, or MONTH
        hds_metadata : See HdsTableMetadataConfig class 
    """
    hds_table_type: HdsTableType # SNAPSHOT OR SCD2
    hds_table_time_partitioning: Optional[TimePartitioning] # DAY, HOUR, or MONTH
    hds_metadata: HdsTableMetadataConfig

    @validator('hds_table_time_partitioning')
    def check_value(cls, v, values):
        if values['hds_table_type'] == HdsTableType.SNAPSHOT:
            assert v is not None, 'Time partitioning must be set for HDS Snapshot tables'
        return v