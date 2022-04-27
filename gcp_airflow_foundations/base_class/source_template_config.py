from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field
from dacite import from_dict


from pydantic import validator, root_validator

import datetime
import re
from typing import List, Optional

from regex import E

from gcp_airflow_foundations.base_class.ods_metadata_config import (
    OdsTableMetadataConfig,
)
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.template_ingestion import TemplateIngestion

from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.base_class.facebook_table_config import FacebookTableConfig
from gcp_airflow_foundations.base_class.template_ingestion_options_config import TemplateIngestionOptionsConfig
from gcp_airflow_foundations.base_class.column_udf_config import ColumnUDFConfig


@dataclass
class SourceTemplateConfig:
    """
    Template configuration data class.

    Attributes:
        table_names : List of table name. Used for Dag Id/s.
        dest_table_override: Optional target table name. If None, use table_name instead.
        surrogate_keys : Keys used to identify unique records when merging into ODS.
        column_mapping : Mapping used to rename columns.
        cluster_fields: The fields used for clustering. BigQuery supports clustering for both partitioned and non-partitioned tables.
        landing_zone_table_name_override_template: Optional staging zone table name template.
        column_casting : Mapping used to cast columns into a specific data type. Note column name uses that of the landing zone table.
        ods_config : ODS table configuration. See :class:`gcp_airflow_foundations.base_class.ods_table_config.OdsTableConfig`.
        hds_config : HDS table confidwguration. See :class:`gcp_airflow_foundations.base_class.hds_table_config.HdsTableConfig`.
        template_ingestion_options: Configuration for template-level ingestion.
        facebook_table_config: Extra options for ingesting data from the Facebook API.
        extra_options: Field for storing additional configuration options.
        start_date : Start date override for DAG
        start_date_tz : Timezone
        version : The Dag version for the table. Can be incremented if logic changes.
        catchup : Passed to a dag [see doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup).
            Defaults to True. May want to change it to False if Dag version is changed, and we don't want to rerun past dags.
    """
    start_date: Optional[str]
    ingestion_type: IngestionType  # FULL or INCREMENTAL
    surrogate_keys: Optional[dict]
    column_mapping: Optional[dict]
    cluster_fields: Optional[List[str]]
    column_casting: Optional[dict]
    new_column_udfs: Optional[dict]
    hds_config: Optional[HdsTableConfig]
    facebook_table_config: Optional[FacebookTableConfig]
    extra_options: dict = field(default_factory=dict)
    dest_table_override: Optional[str] = "{table}"
    landing_zone_table_name_override_template: Optional[str] = "{table}"
    start_date_tz: Optional[str] = "EST"
    ods_config: Optional[OdsTableConfig] = OdsTableConfig(
        ods_metadata=OdsTableMetadataConfig(),
        ods_table_time_partitioning=None,
        partition_column_name=None,
    )
    template_ingestion_options: TemplateIngestionOptionsConfig = TemplateIngestionOptionsConfig(
        ingest_mode="INGEST_BY_TABLE_NAMES",
        ingestion_name="",
        dag_creation_mode="TABLE",
        regex_pattern="",
        table_names=[]
    )
    version: int = 1
    catchup: bool = True

    # Override values
    def __post_init__(self):
        if self.extra_options is None:
            self.extra_options = {}

        if self.landing_zone_table_name_override_template is None:
            self.landing_zone_table_name_override_template = "{table}"

        if self.dest_table_override is None:
            self.dest_table_override = "{table}"    

    @validator("start_date")
    def valid_start_date(cls, v):
        if v is not None:
            assert datetime.datetime.strptime(
                v, "%Y-%m-%d"
            ), "The date format for Start Date should be YYYY-MM-DD"
        return v

    @root_validator(pre=True)
    def valid_hds_dataset(cls, values):
        if (values["cluster_fields"] is not None) and (
            values["column_mapping"] is not None
        ):
            values["cluster_fields"] = [
                values["column_mapping"].get(field, field)
                for field in values["cluster_fields"]
            ]
        return values

    @root_validator(pre=True)
    def valid_column_casting(cls, values):
        if values["column_casting"] is not None:
            assert all(
                [
                    key not in values["surrogate_keys"]
                    for key in values["column_casting"]
                ]
            ), "Column casting is available only for non-key columns."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "Column casting is not currently supported for HDS SCD2 tables."
        return values

    @root_validator(pre=True)
    def valid_new_column_udfs(cls, values):
        if values["new_column_udfs"] is not None:
            assert all(
                [from_dict(data_class=ColumnUDFConfig, data=values["new_column_udfs"][col]) for col in values["new_column_udfs"].keys()]
            ), "New column UDFs must only contain 'function' and 'output_type' keys with corresponding values."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "New column UDFs is not currently supported for HDS SCD2 tables."
        return values

    @root_validator(pre=True)
    def valid_template_ingestion_options(cls, values):
        options = values["template_ingestion_options"]

        assert (
            TemplateIngestion(options["ingest_mode"])
        ), "ingest_mode must be a valid TemplateIngestion Enum"

        template_ingestion = TemplateIngestion(options["ingest_mode"])
        if template_ingestion.value == "INGEST_BY_TABLE_NAME":
            assert (
                not options["regex_pattern"]
            ), "If table_names are explicitly provided for a template, regex_pattern should not be provided in template_ingestion_options"
        elif template_ingestion.value == "INGEST_ALL":
            assert (
                not options["regex_pattern"] and not options["table_names"]
            ), "If a template is ingesting all tables, regex_pattern and table_names should not be provided in template_ingestion_options"
        else:
            assert (
                 re.compile(options["regex_pattern"])
            ), "If ingest_mode is set to 'INGEST_BY_REGX', the regex_pattern should be a valid regex pattern"

        assert (
            options["dag_creation_mode"] in ["TABLE", "SOURCE"]
        ), "dag_creation_mode must be either set to TABLE or SOURCE"

        return values
    