from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class HdsTableMetadataConfig:
    eff_start_time_column_name: Optional[str]
    eff_end_time_column_name: Optional[str]
    status_column_name: Optional[str]

def __post_init__(self):
    if self.eff_start_time_column_name is None:
        self.eff_start_time_column_name = 'eff_start_time'

    if self.eff_end_time_column_name is None:
        self.eff_end_time_column_name = 'eff_end_time'

    if self.status_column_name is None:
        self.status_column_name = 'is_current'
