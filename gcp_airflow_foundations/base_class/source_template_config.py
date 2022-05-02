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
from gcp_airflow_foundations.enums.template_ingestion import TemplateIngestionMode

from gcp_airflow_foundations.base_class.source_base_config import SourceBaseConfig
from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.base_class.template_ingestion_options_config import TemplateIngestionOptionsConfig


@dataclass
class SourceTemplateConfig(SourceBaseConfig):
    """
    Template configuration data class. Template configuration applies to all tables specified within it, either by
    explicitly providing a list of table_names, or by fetching a full list of source tables and optionally filtering
    by regex expression. Templates can create either one DAG per table or one DAG per template.

    Attributes:
        surrogate_keys : Keys used to identify unique records when merging into ODS.
        column_mapping : Mapping used to rename columns.
        cluster_fields: The fields used for clustering. BigQuery supports clustering for both partitioned and non-partitioned tables.
        landing_zone_table_name_override_template: Optional staging zone table name template.
        dest_table_override_template: Optional target table name template. If None, uses table_name instead.
        column_casting : Mapping used to cast columns into a specific data type. Note column name uses that of the landing zone table.
        ods_config : ODS table configuration. See :class:`gcp_airflow_foundations.base_class.ods_table_config.OdsTableConfig`.
        hds_config : HDS table confidwguration. See :class:`gcp_airflow_foundations.base_class.hds_table_config.HdsTableConfig`.
        template_ingestion_options: Configuration for template-level ingestion.
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
    template_ingestion_options: TemplateIngestionOptionsConfig
    extra_options: dict = field(default_factory=dict)
    dest_table_override_template: Optional[str] = "{table}"
    landing_zone_table_name_override_template: Optional[str] = "{table}"
    start_date_tz: Optional[str] = "EST"
    ods_config: Optional[OdsTableConfig] = OdsTableConfig(
        ods_metadata=OdsTableMetadataConfig(),
        ods_table_time_partitioning=None,
        partition_column_name=None,
    )
    version: int = 1
    catchup: bool = True

    # Override values
    def __post_init__(self):
        if self.extra_options is None:
            self.extra_options = {}

        if self.landing_zone_table_name_override_template is None:
            self.landing_zone_table_name_override_template = "{table}"

        if self.dest_table_override_template is None:
            self.dest_table_override_template = "{table}"

    @root_validator(pre=True)
    def valid_template_ingestion_options(cls, values):
        if "template_ingestion_options" in values:
            options = values["template_ingestion_options"]
            template_ingestion = options["ingest_mode"]

            if template_ingestion.value == TemplateIngestionMode.INGEST_BY_TABLE_NAMES:
                assert (
                    not options["regex_pattern"]
                ), "If table_names are explicitly provided for a template, regex_pattern should not be provided in template_ingestion_options"
            elif template_ingestion.value == TemplateIngestionMode.INGEST_ALL:
                assert (
                    not options["regex_pattern"] and not options["table_names"]
                ), "If a template is ingesting all tables, regex_pattern and table_names should not be provided in template_ingestion_options"

            elif template_ingestion.value == TemplateIngestionMode.INGEST_BY_REGEX:
                assert (
                    re.compile(options["regex_pattern"])
                ), "If ingest_mode is set to 'INGEST_BY_REGEX', the regex_pattern should be a valid regex pattern"

            return values
