import pytest

from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs
from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema


class TestSchemaParsing(object):
    """
    Tests that the schema is read from GCS and parsed accordingly based on whether the table is ODS or HDS
    """
    def test_should_read_from_gcs(self, config):
        source = config.source
        for table in config.tables:
            schema_object_temp = source.schema_options.schema_object_template
            source_schema_fields = read_schema_from_gcs(
                gcs_schema_object=schema_object_temp.format(table_name=table.table_name)
            )
            assert source_schema_fields is not None, "Could not read source schema from GCS"

    def test_should_parse_ods_schema(self, config):
        schema_fields = [
            {
                "name":"customerID",
                "type":"STRING"
            },
            {
                "name":"key_id",
                "type":"INTEGER"
            },
            {
                "name":"city_name",
                "type":"STRING"
            }
        ]

        for table in config.tables:
            if table.ods_config:
                schema_fields = parse_ods_schema(
                    schema_fields=schema_fields,
                    ods_metadata=table.ods_config.ods_metadata
                )
                assert schema_fields is not None, "Could not parse ODS schema fields"

    def test_should_parse_hds_schema(self, config): 
        schema_fields = [
            {
                "name":"customerID",
                "type":"STRING"
            },
            {
                "name":"key_id",
                "type":"INTEGER"
            },
            {
                "name":"city_name",
                "type":"STRING"
            }
        ]
        
        for table in config.tables:
            if table.hds_config:
                schema_fields = parse_hds_schema(
                    schema_fields=schema_fields,          
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )
                assert schema_fields is not None, "Could not parse HDS schema fields"         