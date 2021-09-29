from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class HdsTableMetadataConfig:
    eff_start_time_column_name: Optional[str] = 'af_metadata_created_at'
    eff_end_time_column_name: Optional[str] = 'af_metadata_expired_at'
    hash_column_name: Optional[str] = 'af_metadata_row_hash'
