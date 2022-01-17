from dataclasses import dataclass, field
from typing import Optional

from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig


@dataclass
class DlpTableConfig:
    """
    Attributes:
    """

    template_name_override: Optional[str] = None
    rows_limit_percent_override: Optional[int] = None
    min_match_count_override: Optional[int] = None
    recurrence_override: Optional[str] = None


    def get_template_name(self, source_config: DlpSourceConfig):
        return self.template_name_override if self.template_name_override else source_config.template_name

    def get_rows_limit_percent(self, source_config: DlpSourceConfig):
        return self.rows_limit_percent_override if self.rows_limit_percent_override \
            else source_config.rows_limit_percent

    def get_min_match_count(self, source_config: DlpSourceConfig):
        return self.min_match_count_override if self.min_match_count_override \
            else source_config.min_match_count

    def get_recurrence(self, source_config: DlpSourceConfig):
        return self.recurrence_override if self.recurrence_override \
            else source_config.recurrence