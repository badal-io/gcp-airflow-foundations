from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class OdsTableMetadataConfig:
    hash_column_name: Optional[str]
    primary_key_hash_column_name: Optional[str]
    ingestion_time_column_name: Optional[str]
    update_time_column_name: Optional[str]

def __post_init__(self):
    if self.hash_column_name is None:
        self.hash_column_name = 'af_metadata_row_hash'

    if self.primary_key_hash_column_name is None:
        self.primary_key_hash_column_name = 'af_metadata_primary_key_hash'

    if self.ingestion_time_column_name is None:
        self.ingestion_time_column_name = 'af_metadata_inserted_at'

    if self.update_time_column_name is None:
        self.update_time_column_name = 'af_metadata_updated_at'