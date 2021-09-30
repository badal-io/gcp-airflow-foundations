from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class HdsTableMetadataConfig:
    """
    Attributes:
        eff_start_time_column_name : Insertion time column name
        eff_end_time_column_name: Expiry time column name
        partition_time_column_name: Partition time column name
        hash_column_name : Hash column name
    """
    eff_start_time_column_name: str = 'af_metadata_created_at'
    eff_end_time_column_name: str = 'af_metadata_expired_at'
    partition_time_column_name: str = 'partition_time'
    hash_column_name: str = 'af_metadata_row_hash'
