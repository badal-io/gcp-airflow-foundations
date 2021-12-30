from dataclasses import dataclass, field
from pydantic import validator, root_validator

@dataclass
class DlpSourceConfig:
    """
    Attributes:
         results_dataset_id: Dataset for temporarily storing dlp results
    """

    results_dataset_id: str
    template_name: str
    rows_limit_percent: int
    min_match_count:int
