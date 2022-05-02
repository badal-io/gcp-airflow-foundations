from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from datetime import datetime
from typing import List, Optional
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.base_class.source_template_config import SourceTemplateConfig
from gcp_airflow_foundations.base_class.source_config import SourceConfig
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning

import logging


partitioning_options = {"HOUR": "@hourly", "DAY": "@daily", "MONTH": "@monthly"}


@dataclass
class DataSourceTablesConfig:
    """
    Main configuration class used to map the fields from the configuration file.

    Attributes:
        source: The data source configuration
        tables: The table configuration
    """

    source: SourceConfig
    tables: Optional[List[SourceTableConfig]]
    templates: Optional[List[SourceTemplateConfig]]

    def __post_init__(self):
        if self.templates is None:
            self.templates = []
        if self.tables is None:
            self.tables = []

    @root_validator(pre=True)
    def valid_partitioning(cls, values):
        ingest_schedule = values["source"].ingest_schedule

        for table in values["tables"]:
            if (
                (table.hds_config is not None)
                and (table.hds_config.hds_table_time_partitioning is not None)
                and (table.hds_config.hds_table_type == HdsTableType.SNAPSHOT)
            ):
                partitioning_time = table.hds_config.hds_table_time_partitioning.value
                assert (
                    partitioning_options[partitioning_time] == ingest_schedule
                ), f"Invalid partitioning time selection for table `{table.table_name}` - partitioning time `{partitioning_time}` must match ingestion schedule `{ingest_schedule}`"

        return values

    @root_validator(pre=True)
    def valid_config(cls, values):
        assert (
            values["tables"] or values["templates"]
        ), "At least one of tables or templates should be present in the config file (non-empty)"

        return values

    def dagrun_timeout(self):
        return timedelta(minutes=self.source.acceptable_delay_minutes)

    def table_start_date(self, table_config: SourceTableConfig):
        if table_config.start_date:
            start_date = table_config.start_date
        else:
            start_date = self.source.start_date

        return datetime.strptime(start_date, "%Y-%m-%d")

    def source_start_date(self):
        return datetime.strptime(self.source.start_date, "%Y-%m-%d")
