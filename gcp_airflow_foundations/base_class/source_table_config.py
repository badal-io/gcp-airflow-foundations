from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from pydantic import validator, root_validator

import datetime
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
    Table configuration data class.

    Attributes:
        table_name : Table name. Used for Dag Id.
        ingestion_type: FULL or INCREMENTAL.
        landing_zone_table_name_override: Optional staging zone table name.
        dest_table_override: Optional target table name. If None, use table_name instead.
        surrogate_keys : Keys used to identify unique records when merging into ODS.
        column_mapping : Mapping used to rename columns.
        cluster_fields: The fields used for clustering. BigQuery supports clustering for both partitioned and non-partitioned tables.
        column_casting : Mapping used to cast columns into a specific data type. Note column name uses that of the landing zone table.
        ods_config : ODS table configuration. See :class:`gcp_airflow_foundations.base_class.ods_table_config.OdsTableConfig`.
        hds_config : HDS table configuration. See :class:`gcp_airflow_foundations.base_class.hds_table_config.HdsTableConfig`.
        facebook_table_config: Extra options for ingesting data from the Facebook API.
        extra_options: Field for storing additional configuration options.
        start_date : Start date override for DAG
        start_date_tz : Timezone
        version : The Dag version for the table. Can be incremented if logic changes.
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
    """

    table_name: str
    ingestion_type: IngestionType # FULL or INCREMENTAL
    landing_zone_table_name_override: Optional[str]
    dest_table_override: Optional[str]
    surrogate_keys: List[str]
    column_mapping: Optional[dict]
    cluster_fields: Optional[List[str]]
    column_casting: Optional[dict]
    hds_config: Optional[HdsTableConfig]
    facebook_table_config: Optional[FacebookTableConfig]
    start_date: Optional[str]
    extra_options: dict = field(default_factory=dict)
    start_date_tz: Optional[str] = "EST"
    ods_config: Optional[OdsTableConfig] = OdsTableConfig(ods_metadata=OdsTableMetadataConfig(), ods_table_time_partitioning=None, partition_column_name=None)
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

    @validator("start_date")
    def valid_start_date(cls, v):
        if v is not None:
            assert datetime.datetime.strptime(v, "%Y-%m-%d"), \
                "The date format for Start Date should be YYYY-MM-DD"
        return v

    @root_validator(pre=True)
    def valid_hds_dataset(cls, values):
        if (values['cluster_fields'] is not None) and (values['column_mapping'] is not None):
            values['cluster_fields'] = [
                values['column_mapping'].get(field, field) for field in values['cluster_fields']
            ]
        return values

    @root_validator(pre=True)
    def valid_column_casting(cls, values):
        if values['column_casting'] is not None:
            assert all([key not in values['surrogate_keys'] for key in values['column_casting']]), "Column casting is available only for non-key columns."

            if values['hds_config'] is not None:
                assert values['hds_config'].hds_table_type != HdsTableType.SCD2, "Column casting is not currently supported for HDS SCD2 tables."
        return values