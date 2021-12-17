from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class FTPTableConfig:
    """
    Attributes:
        directory_prefix: optional directory prefix override per table
        metadata_file: the relative path to the metadata file within the GSC bucket
        schema_file: the (optional) relative path to the schema file for the table
        source_format: file format for all files in this metadata file
        date_table: optional date column name for external partitions for SFTP directory transfer
    """
    directory_prefix: Optional[str]
    metadata_file: Optional[str]
    schema_file: Optional[str]
    source_format: str
    flag_file_path: Optional[str]