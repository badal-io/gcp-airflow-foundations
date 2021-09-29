from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class OdsTableMetadataConfig:
    hash_column_name: Optional[str] = 'af_metadata_row_hash'
    primary_key_hash_column_name: Optional[str] = 'af_metadata_primary_key_hash'
    ingestion_time_column_name: Optional[str] = 'af_metadata_inserted_at'
    update_time_column_name: Optional[str] = 'af_metadata_updated_at'
