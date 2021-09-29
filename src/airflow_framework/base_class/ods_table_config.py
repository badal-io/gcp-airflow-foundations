from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional

from airflow_framework.base_class.ods_metadata_config import OdsTableMetadataConfig
from airflow_framework.enums.ingestion_type import IngestionType

@dataclass
class OdsTableConfig:
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
    ods_metadata: OdsTableMetadataConfig
    ingestion_type: IngestionType = "FULL" # FULL or INCREMENTAL
    merge_type: str = "SG_KEY_WITH_HASH"
