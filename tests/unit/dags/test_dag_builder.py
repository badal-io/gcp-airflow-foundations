import os
import pytest
import unittest

from gcp_airflow_foundations.source_class.source import DagBuilder

from gcp_airflow_foundations.source_class import (
    sftp_source,
    gcs_source,
    jdbc_dataflow_source,
    oracle_dataflow_source,
    salesforce_source,
    twilio_source,
    facebook
)


class TestDagBuilder(unittest.TestCase):
    def setUp(self):
        self.sources = [
            twilio_source.TwilioToBQDagBuilder,
            facebook.FacebooktoBQDagBuilder,
            sftp_source.SFTPFileIngestionDagBuilder,
            gcs_source.GCSFileIngestionDagBuilder,
            jdbc_dataflow_source.JdbcToBQDataflowDagBuilder,
            oracle_dataflow_source.OracleToBQDataflowDagBuilder,
            salesforce_source.SalesforcetoBQDagBuilder
        ]

    def test_dag_builder(self):
        assert all([source in DagBuilder.sources for source in self.sources])