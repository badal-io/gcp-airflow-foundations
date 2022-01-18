from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class SFTPTableConfig:
    """
    Attributes:
        full_dir_download: 
        date_column: optional date column name for scanning/replacing in external partitions for .PARQUET uploads
    """
    full_dir_download: bool
    date_column: Optional[str]
    