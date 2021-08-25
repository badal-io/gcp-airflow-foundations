from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional


@dataclass
class OdsTableConfig:
    """A bird with a flight speed exceeding that of an unladen swallow.

   Attributes:
        table_name : Table name. Used for Dag Id
        ods_table_name_override: Optional ods table name. If None, use table_name instead
        table_type : Reserved for future use. For now only valid value is SCD_TYPE2
        surrogate_keys : Keys used to identify unique records when merging into ODS
        update_columns : Columns to update for existing records when merging
        ods_partition : BigQuery partitioning schema for ODS data table (should not be changed after first run )
        object_prefix : The prefix of the GCS files with the extracted data/schema
        write_mode : Reserved for future use. Only 'UPSERT' is supported now
        transformations : Optional list of transformations to perform after to load
        validations : Optional list of data validations to perform on the ODS data
        ods_add_cdc_fields : Whether to add cdc (hash, inserted_ts, etc.) to recrods inserted into ODS
        version : The Dag version for the table. Can be incremented if logic changes
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
   """

    table_name: str
    sink_table_name: str
    schema: List[dict]
    #surrogate_keys: List[str]
    #update_columns: List[str]
    #object_prefix: str
    #write_mode: LoadWriteMode
    #transformations: List[Transformation]
    #sla_mins: Optional[int]
    #extra_options: dict = field(default_factory=dict)
    #ods_add_cdc_fields: bool = False
    version: int = 1
    catchup: bool = True
    #start_date_override: Optional[str] = None