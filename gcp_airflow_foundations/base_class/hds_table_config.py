from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator, root_validator

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.hds_metadata_config import HdsTableMetadataConfig
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning

@dataclass
class HdsTableConfig:
    """
    Attributes:
        hds_table_type : SNAPSHOT OR SCD2
        hds_table_time_partitioning: Partitioning for BigQuery table. One of DAY, HOUR, or MONTH
        hds_metadata : See HdsTableMetadataConfig class 
    """

    hds_metadata: HdsTableMetadataConfig
    hds_table_time_partitioning: Optional[TimePartitioning] # DAY, HOUR, or MONTH
    hds_table_type: HdsTableType = HdsTableType.SCD2 # SNAPSHOT OR SCD2

    @root_validator(pre=True)
    def valid_config(cls, values):
        if values['hds_table_type'] == HdsTableType.SNAPSHOT:
            assert values['hds_table_time_partitioning'] is not None, 'HDS snapshot table\'s partition time must match the ingestion schedule'
        return values