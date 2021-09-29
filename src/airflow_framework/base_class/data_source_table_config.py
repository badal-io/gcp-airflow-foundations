from dataclasses import dataclass
from datetime import timedelta
from datetime import datetime

from typing import List

from airflow_framework.base_class.source_table_config import SourceTableConfig
from airflow_framework.base_class.source_config import SourceConfig

import logging


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
