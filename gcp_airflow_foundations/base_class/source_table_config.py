from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from pydantic import validator

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig
from gcp_airflow_foundations.base_class.hds_metadata_config import HdsTableMetadataConfig
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning

from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.base_class.facebook_table_config import FacebookTableConfig

@dataclass
class SourceTableConfig:
    """
    Attributes:
        table_name : Table name. Used for Dag Id
        ingestion_type: FULL or INCREMENTAL
        landing_zone_table_name_override: Optional staging zone table name.
        dest_table_override: Optional target table name. If None, use table_name instead
        surrogate_keys : Keys used to identify unique records when merging into ODS
        column_mapping : Mapping used to rename columns
        ods_config : See OdsTableConfig
        hds_config : See HdsTableConfig
        version : The Dag version for the table. Can be incremented if logic changes
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
    """

    table_name: str
    ingestion_type: IngestionType # FULL or INCREMENTAL
    landing_zone_table_name_override: Optional[str]
    dest_table_override: Optional[str]
    surrogate_keys: List[str]
    column_mapping: Optional[dict]
    hds_config: Optional[HdsTableConfig]
    facebook_table_config: Optional[FacebookTableConfig]
    extra_options: dict = field(default_factory=dict)
    ods_config: Optional[OdsTableConfig] = OdsTableConfig(ods_metadata=OdsTableMetadataConfig())
    version: int = 1
    catchup: bool = True


    # Override values
    def __post_init__(self):
        if self.landing_zone_table_name_override is None:
            self.landing_zone_table_name_override = self.table_name
        if self.extra_options is None:
            self.extra_options = {}
            

    @validator("table_name")
    def valid_source_table(cls, v):
        assert v, "Source table name must not be empty"
        assert "." not in v, "Source table name cannot contain the period character"
        return v