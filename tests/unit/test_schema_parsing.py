import pytest

from airflow_framework.common.gcp.source_schema.gcs import read_schema_from_gcs
from airflow_framework.common.gcp.ods.schema_utils import parse_ods_schema
from airflow_framework.common.gcp.hds.schema_utils import parse_hds_schema


class TestSchemaParsing(object):
    """
    Tests that the schema is read from GCS and parsed accordingly based on whether the table is ODS or HDS
    """
    def test_should_read_from_gcs(self, config):
        for table in config.tables:
            source_schema_fields = read_schema_from_gcs(
                gcs_schema_object=table.source_table_schema_object,
                schema_fields=None,
                column_mapping=table.column_mapping
            )
            assert source_schema_fields is not None, "Could not read source schema from GCS"

    def test_should_parse_ods_schema(self, config):
        for table in config.tables:
            if table.ods_config:
                schema_fields, _ = parse_ods_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )
                assert schema_fields is not None, "Could not parse ODS schema fields"

    def test_should_parse_hds_schema(self, config): 
        for table in config.tables:
            if table.hds_config:
                schema_fields, _ = parse_hds_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,            
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )
                assert schema_fields is not None, "Could not parse HDS schema fields"         