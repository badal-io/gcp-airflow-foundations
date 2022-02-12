from dataclasses import dataclass, field
from typing import Optional

from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig


@dataclass
class DlpTableConfig:
    """
    Attributes:
    """

    is_on: Optional[bool] = None
    template_name_override: Optional[str] = None
    rows_limit_percent_override: Optional[int] = None
    min_match_count_override: Optional[int] = None
    recurrence_override: Optional[str] = None

    def set_source_config(self, source_config: DlpSourceConfig):
        self.source_config = source_config

    def get_is_on(self):
        return self.is_on if self.is_on else (self.source_config is not None)

    def get_template_name(self):
        return (
            self.template_name_override
            if self.template_name_override
            else self.source_config.template_name
        )

    def get_rows_limit_percent(self):
        return (
            self.rows_limit_percent_override
            if self.rows_limit_percent_override
            else self.source_config.rows_limit_percent
        )

    def get_min_match_count(self):
        return (
            self.min_match_count_override
            if self.min_match_count_override
            else self.source_config.min_match_count
        )

    def get_recurrence(self):
        return (
            self.recurrence_override
            if self.recurrence_override
            else self.source_config.recurrence_schedule
        )
