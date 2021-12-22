import os
import pytest
import unittest

from hashlib import sha256

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.operators.gcp.ods.ods_sql_upsert_helpers import SqlHelperODS
from gcp_airflow_foundations.operators.gcp.hds.hds_sql_upsert_helpers import SqlHelperHDS
from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig
from gcp_airflow_foundations.base_class.hds_metadata_config import HdsTableMetadataConfig


class TestSqlHelperODS(unittest.TestCase):
    def setUp(self):
        kwargs = {
            'source_dataset': 'source_dataset',
            'target_dataset': 'target_dataset',
            'source': 'source',
            'target': 'target',
            'surrogate_keys': ['key'],
            'column_mapping': {'key':'key', 'column_a':'column_b'},
            'ods_metadata': OdsTableMetadataConfig(),
            'columns': ['key', 'column_a']
        }

        self.sql_helper = SqlHelperODS(
            **kwargs
        )

    def test_full_sql(self):
        assert self.sql_helper.create_full_sql() == """
            SELECT `key` AS `key`,`column_a` AS `column_b`,
                CURRENT_TIMESTAMP() AS af_metadata_inserted_at,
                CURRENT_TIMESTAMP() AS af_metadata_updated_at, 
                TO_BASE64(MD5(TO_JSON_STRING(S))) AS af_metadata_row_hash, 
                TO_BASE64(MD5(ARRAY_TO_STRING([CAST(S.`key` AS STRING)], ""))) AS af_metadata_primary_key_hash
            FROM `source_dataset.source` S
        """

    def test_upsert_sql(self):
        assert self.sql_helper.create_upsert_sql_with_hash() == """
            MERGE `target_dataset.target` T
            USING `source_dataset.source` S
            ON T.key=S.key
            WHEN MATCHED THEN UPDATE
                SET `key`=S.`key`,`column_b`=S.`column_a`,af_metadata_updated_at=CURRENT_TIMESTAMP(),af_metadata_row_hash=TO_BASE64(MD5(TO_JSON_STRING(S)))
            WHEN NOT MATCHED THEN
                INSERT (`key`,`column_b`, af_metadata_inserted_at, af_metadata_updated_at, af_metadata_row_hash, af_metadata_primary_key_hash)
                VALUES (`key`,`column_a`, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(ARRAY_TO_STRING([CAST(S.`key` AS STRING)], ""))))
        """