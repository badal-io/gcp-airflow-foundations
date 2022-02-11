import unittest
from gcp_airflow_foundations.operators.gcp.dlp.dlp_helpers import (
    results_to_bq_policy_tags,
)
from gcp_airflow_foundations.base_class.dlp_source_config import PolicyTagConfig


class TestDlpSubTask(unittest.TestCase):
    def test_results_to_bq_policy_tags(self):
        poicy_tag_config = PolicyTagConfig(
            taxonomy="test", location="us", tag="test_tag"
        )

        dlp_results = [
            ["email", "EMAIL_ADDRESS", "VERY_LIKELY", 40],
            ["email", "DOMAIN_NAME", "LIKELY", 40],
            ["city_name", "LOCATION", "POSSIBLE", 40],
        ]
        tags = results_to_bq_policy_tags("project_id", dlp_results, poicy_tag_config)
        expected_tag = (
            "projects/project_id/locations/us/taxonomies/test/policyTags/test_tag"
        )
        assert tags == [
            {"name": "email", "policy_tags": {"names": [expected_tag]}},
            {"name": "city_name", "policy_tags": {"names": [expected_tag]}},
        ]
