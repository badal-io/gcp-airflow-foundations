from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class SFTPTableConfig:
    """
    Attributes:
        sftp_connection_name: name of the connection in Airflow
        date_column: optional date column name for scanning/replacing in external partitions for .PARQUET uploads
    """
    sftp_connection_name: str
    sftp_private_key_secret_name: Optional[str]
    date_column: Optional[str]
    