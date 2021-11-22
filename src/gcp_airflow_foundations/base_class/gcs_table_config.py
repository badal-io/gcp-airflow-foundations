from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class GCSTableConfig:
    """
    Attributes:
        metadata_file: the relative path to the metadata file within the GSC bucket
        schema_file: the (optional) relative path to the schema file for the table
        source_format: file format for all files in this metadata file
        file_prefix_filtering: option to search for file by prefix (e.g. if the end of file name is a randomly generated string.)
    """
    metadata_file: Optional[str]
    schema_file: Optional[str]
    source_format: str
    directory_prefix: Optional[str]