from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

from pydantic.networks import stricturl

@dataclass
class GCSSourceConfig:
    """
    Attributes:
        template_file_name: a templated file name that applies to all tables. The DAGs will search for one file per table accordingly,
            and no metadata file is required if this field is supplied.
        file_prefix_filtering: option to search for file by prefix (e.g. if the end of file name is a randomly generated string.)
    """
    template_file_name: Optional[str]
    directory_prefix: Optional[str]
    file_prefix_filtering: bool
    date_format: str
    delimeter: str
    source_format: str