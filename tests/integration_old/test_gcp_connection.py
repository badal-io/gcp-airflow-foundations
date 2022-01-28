from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class TestGBQConnectorIntegration(object):
    """ Tests that the connection to BQ is established without errors """

    def test_should_connect_to_gcp(self):
        gcp_conn_id = "google_cloud_default"
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        assert (
            bq_hook.get_conn() is not None
        ), "Could not establish a connection to BigQuery"
