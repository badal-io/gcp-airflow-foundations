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


class TestSqlHelperHDS(unittest.TestCase):
    def setUp(self):
        kwargs = {
            'source_dataset': 'source_dataset',
            'target_dataset': 'target_dataset',
            'source': 'source',
            'target': 'target',
            'surrogate_keys': ['key'],
            'column_mapping': {'key':'key', 'column_a':'column_b'},
            'hds_metadata': HdsTableMetadataConfig(),
            'columns': ['key', 'column_a']
        }

        self.sql_helper = SqlHelperHDS(
            **kwargs
        )

    def test_incremental_scd2(self):
        ingestion_type = IngestionType.INCREMENTAL
        print(self.sql_helper.create_scd2_sql_with_hash(ingestion_type))
        assert self.sql_helper.create_scd2_sql_with_hash(ingestion_type) == """
            MERGE `target_dataset.target` T
            USING (SELECT  key AS join_key_key, key,column_a
            FROM `source_dataset.source`
            UNION ALL 
            SELECT
                source.`key`,
                NULL,source.`column_a`
                FROM `source_dataset.source` source
                JOIN `target_dataset.target` target
                ON target.key=source.key
                WHERE ( 
                        MD5(TO_JSON_STRING(target.`column_b`)) != MD5(TO_JSON_STRING(source.`column_a`))
                        AND target.af_metadata_expired_at IS NULL
                    )) S
            ON T.key=S.key
            WHEN MATCHED AND MD5(TO_JSON_STRING(T.`column_b`)) != MD5(TO_JSON_STRING(S.`column_a`)) THEN UPDATE SET af_metadata_expired_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED BY TARGET THEN INSERT (`key`,`column_b`, af_metadata_created_at, af_metadata_expired_at, af_metadata_row_hash)
            VALUES (join_key_key,`column_a`, CURRENT_TIMESTAMP(), NULL, TO_BASE64(MD5(TO_JSON_STRING(S)))) 
            
        """

    def test_full_scd2(self):
        ingestion_type = IngestionType.FULL
        print(self.sql_helper.create_scd2_sql_with_hash(ingestion_type=ingestion_type))
        assert self.sql_helper.create_scd2_sql_with_hash(ingestion_type=ingestion_type) == """
            MERGE `target_dataset.target` T
            USING (SELECT  key AS join_key_key, key,column_a
            FROM `source_dataset.source`
            UNION ALL 
            SELECT
                source.`key`,
                NULL,source.`column_a`
                FROM `source_dataset.source` source
                JOIN `target_dataset.target` target
                ON target.key=source.key
                WHERE ( 
                        MD5(TO_JSON_STRING(target.`column_b`)) != MD5(TO_JSON_STRING(source.`column_a`))
                        AND target.af_metadata_expired_at IS NULL
                    )) S
            ON T.key=S.key
            WHEN MATCHED AND MD5(TO_JSON_STRING(T.`column_b`)) != MD5(TO_JSON_STRING(S.`column_a`)) THEN UPDATE SET af_metadata_expired_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED BY TARGET THEN INSERT (`key`,`column_b`, af_metadata_created_at, af_metadata_expired_at, af_metadata_row_hash)
            VALUES (join_key_key,`column_a`, CURRENT_TIMESTAMP(), NULL, TO_BASE64(MD5(TO_JSON_STRING(S)))) 
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET T.af_metadata_expired_at = CURRENT_TIMESTAMP()
        """

    def test_snapshot(self):
        partition_timestamp = '2021-01-01T00:00:00'

        self.sql_helper.partition_column_name = 'partition_column'
        self.sql_helper.time_partitioning = 'DAY'
        print(self.sql_helper.create_snapshot_sql_with_hash(partition_timestamp=partition_timestamp))
        assert self.sql_helper.create_snapshot_sql_with_hash(partition_timestamp=partition_timestamp) == """
            SELECT
                `key` AS `key`,`column_a` AS `column_b`,
                CURRENT_TIMESTAMP() AS af_metadata_created_at,
                TIMESTAMP_TRUNC('2021-01-01T00:00:00', DAY) AS partition_column,
                TO_BASE64(MD5(TO_JSON_STRING(S))) AS af_metadata_row_hash
            FROM source_dataset.source S
        """