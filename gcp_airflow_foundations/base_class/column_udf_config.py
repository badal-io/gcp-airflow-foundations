from pydantic.dataclasses import dataclass

from typing import List, Optional


@dataclass
class ColumnUDFConfig:
    """
    Attributes:
        function: BigQuery function to generate the additional column
        output_type: Bigquery datatype
    """

    function: str
    output_type: str
