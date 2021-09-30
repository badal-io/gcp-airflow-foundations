from pydantic import validator
from dataclasses import dataclass
from datetime import timedelta
from datetime import datetime

from typing import List

from airflow_framework.base_class.source_table_config import SourceTableConfig
from airflow_framework.base_class.source_config import SourceConfig
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.enums.time_partitioning import TimePartitioning

import logging

partitioning_options = {
    "HOUR":"@hourly",
    "DAY":"@daily", 
    "MONTH":"@monthly"
}


@dataclass
class DataSourceTablesConfig:
    source: SourceConfig
    tables: List[SourceTableConfig]

    def dagrun_timeout(self):
        return timedelta(minutes=self.source.acceptable_delay_minutes)

    def table_start_date(self, table_config: SourceTableConfig):
        return datetime.strptime(self.source.start_date, "%Y-%m-%d")

    def source_start_date(self):
        return datetime.strptime(self.source.start_date, "%Y-%m-%d")

    @validator('tables')
    def check_partitioning(cls, v, values):
        ingest_schedule = values['source'].ingest_schedule

        for table in v:
            if (table.hds_config is not None) and (table.hds_config.hds_table_type == HdsTableType.SNAPSHOT):
                assert partitioning_options[table.hds_config.hds_table_time_partitioning.value] == ingest_schedule, \
                    "Invalid partitioning time selection - partitioning time must match ingestion schedule"