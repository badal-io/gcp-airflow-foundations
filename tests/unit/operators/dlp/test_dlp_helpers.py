import unittest
from gcp_airflow_foundations.operators.gcp.dlp.dlp_helpers import results_to_bq_policy_tags

class TestDlpSubTask(unittest.TestCase):
    def test_results_to_bq_policy_tags(self):
        dlp_results = [['email', 'EMAIL_ADDRESS', 'VERY_LIKELY', 40], ['email', 'DOMAIN_NAME', 'LIKELY', 40], ['city_name', 'LOCATION', 'POSSIBLE', 40]]
        tags = results_to_bq_policy_tags(dlp_results, "test_tag")
        assert tags == [{'name': 'email', 'policy_tags': {'names': ['test_tag']}},
                        {'name': 'city_name','policy_tags': {'names': ['test_tag']}}]

