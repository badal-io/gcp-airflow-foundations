from dataclasses import dataclass, field
from pydantic import validator, root_validator

@dataclass
class PolicyTagConfig:
    taxonomy: str
    location: str
    tag: str

@dataclass
class DlpSourceConfig:
    """
    Attributes:
         results_dataset_id: Dataset for temporarily storing dlp results
    """

    results_dataset_id: str
    template_name: str
    rows_limit_percent: int
    policy_tag_config: PolicyTagConfig
    min_match_count:int = 1
    recurrence_schedule: str = "0 0 1 * *" # First day of every month
    run_on_first_execution: bool = True


