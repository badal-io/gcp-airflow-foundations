from dataclasses import dataclass, field
from pydantic import validator, root_validator
from typing import Optional

from base_class.dlp_source_config import DlpSourceConfig


@dataclass
class DlpTableConfig:
    """
    Attributes:
    """

    template_name_override: Optional[str]
    rows_limit_percent_override: Optional[int]
    min_match_count_override: Optional[int]


    def get_template_name(self, source_config: DlpSourceConfig):
        return self.template_name_override if self.template_name_override else source_config.template_name

    def get_rows_limit_percent(self, source_config: DlpSourceConfig):
        return self.rows_limit_percent_override if self.rows_limit_percent_override \
            else source_config.rows_limit_percent

    def get_min_match_count(self, source_config: DlpSourceConfig):
        return self.min_match_count_override if self.min_match_count_override \
            else source_config.min_match_count
