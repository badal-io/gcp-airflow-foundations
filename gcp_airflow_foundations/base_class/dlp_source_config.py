from dataclasses import dataclass, field
from pydantic import validator, root_validator


@dataclass
class PolicyTagConfig:
    """
    Information about a policy tag to apply the columns
    See the following for a good example of creating a taxonomy: https://cloud.google.com/bigquery/docs/best-practices-policy-tags
    Attributes:
         taxonomy: The taxonomy to use: https://cloud.google.com/data-catalog/docs/samples/data-catalog-ptm-create-taxonomy
         location: GCP logcation where the taxonomy was created
         tag: The tag to use (must be part of the taxonomy)
    """

    taxonomy: str
    location: str
    tag: str


@dataclass
class DlpSourceConfig:
    """
    Attributes:
         results_dataset_id:  Dataset for temporarily storing dlp results
         template_name: Template to use when running DLP. Full path is expected: projects/{prj_id}/locations/{location}}/inspectTemplates/{template}
         rows_limit_percent: What percentage of rows to scan/sample
         policy_tag_config: Policy tag to use for tagging columns
         min_match_count: Min numbers of rows that must match and InfoType for the column to be tagged
         recurrence_schedule: How often to run the DLP scan and update the tags
         run_on_first_execution: True if we should run on the first time we ingest a new table
    """

    results_dataset_id: str
    template_name: str
    policy_tag_config: PolicyTagConfig
    rows_limit_percent: int = 10
    min_match_count: int = 1
    recurrence_schedule: str = "0 0 1 * *"  # First day of every month
    run_on_first_execution: bool = True
