from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional

from airflow_framework.base_class.hds_metadata_config import HdsTableMetadataConfig
from airflow_framework.enums.hds_table_type import HdsTableType

@dataclass
class HdsTableConfig:
    """
    Attributes:
        table_name : Table name. Used for Dag Id
        ods_table_name_override: Optional ods table name. If None, use table_name instead
        table_type : Reserved for future use. For now only valid value is SCD_TYPE2
        surrogate_keys : Keys used to identify unique records when merging into ODS
        ods_partition : BigQuery partitioning schema for ODS data table (should not be changed after first run )
        version : The Dag version for the table. Can be incremented if logic changes
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
    """
    hds_table_type: HdsTableType # SNAPSHOT OR SCD2
    hds_metadata: HdsTableMetadataConfig
