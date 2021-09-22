from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class HdsTableMetadataConfig:
    eff_start_time_column_name: Optional[str]
    eff_end_time_column_name: Optional[str]
    status_column_name: Optional[str]
    hash_column_name: Optional[str]

def __post_init__(self):
    if self.eff_start_time_column_name is None:
        self.eff_start_time_column_name = 'af_metadata_inserted_at'

    if self.eff_end_time_column_name is None:
        self.eff_end_time_column_name = 'af_metadata_updated_at'

    if self.status_column_name is None:
        self.status_column_name = 'is_current'

    if self.hash_column_name is None:
        self.hash_column_name = 'af_metadata_row_hash'