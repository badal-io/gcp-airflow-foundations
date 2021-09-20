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
        update_columns : Columns to update for existing records when merging
        ods_partition : BigQuery partitioning schema for ODS data table (should not be changed after first run )
        object_prefix : The prefix of the GCS files with the extracted data/schema
        transformations : Optional list of transformations to perform after to load
        validations : Optional list of data validations to perform on the ODS data
        ods_add_cdc_fields : Whether to add cdc (hash, inserted_ts, etc.) to recrods inserted into ODS
        version : The Dag version for the table. Can be incremented if logic changes
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
    """

    table_name: str
    landing_zone_table_name_override: Optional[str]
    source_table_schema_object: Optional[str]
    ingestion_type: IngestionType # FULL or INCREMENTAL
    merge_type: Optional[str]
    surrogate_keys: List[str]
    update_columns: List[str]
    column_mapping: Optional[dict]
    dest_table_override: Optional[str]
    ods_metadata: OdsTableMetadataConfig
    version: int = 1
    catchup: bool = True


    # Override values for optional fields
    def __post_init__(self):
        if self.landing_zone_table_name_override is None:
            self.landing_zone_table_name_override = self.table_name

        if self.merge_type is None:
            self.merge_type = "SG_KEY_WITH_HASH"