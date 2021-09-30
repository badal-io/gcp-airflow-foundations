from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class OdsTableMetadataConfig:
    """
    Attributes:
        hash_column_name : Hash column name
        primary_key_hash_column_name: Primary key hash column name
        ingestion_time_column_name : Insertion time column name
        update_time_column_name : Update time column name
    """
    hash_column_name: str = 'af_metadata_row_hash'
    primary_key_hash_column_name: str = 'af_metadata_primary_key_hash'
    ingestion_time_column_name: str = 'af_metadata_inserted_at'
    update_time_column_name: str = 'af_metadata_updated_at'
