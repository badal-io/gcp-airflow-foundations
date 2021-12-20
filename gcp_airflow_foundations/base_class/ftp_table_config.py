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
        flag_file_path: the path to the optional success file
        bq_upload_option: either "BASH" or "GCS"
    """
    directory_prefix: Optional[str]
    metadata_file: Optional[str]
    schema_file: Optional[str]
    flag_file_path: Optional[str]
    parquet_upload_option: Optional[str]