import pytest

from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema


class TestSchemaMigrationOperator(object):
    """
    Tests that the SchemaMigration Operator successfully returns a list of SQL statemets for migrating the schema of the test table columns
    """
    @pytest.fixture(autouse=True)
    def setup(self, project_id, target_dataset, config):
        self.project_id = project_id
        self.target_dataset = target_dataset
        self.config = config

    def test_should_pick_columns_for_ods_schema_migration(self):
        source = self.config.source
        for table in self.config.tables:
            ingestion_type = table.ingestion_type
            if table.ods_config:

                if ingestion_type == IngestionType.INCREMENTAL:
                    table_id = f"{table.table_name}_ODS_Incremental"
                    expected_query = "SELECT `customerID`,`key_id`,`city_name`,`af_metadata_inserted_at`,`af_metadata_updated_at`,`af_metadata_primary_key_hash`,`af_metadata_row_hash` FROM `airflow_test.customer_data_ODS_Incremental`;"

                elif ingestion_type == IngestionType.FULL:
                    table_id = f"{table.table_name}_ODS_Full"
                    expected_query = "SELECT `customerID`,`key_id`,`city_name`,`af_metadata_inserted_at`,`af_metadata_updated_at`,`af_metadata_row_hash`,`af_metadata_primary_key_hash` FROM `airflow_test.customer_data_ODS_Full`;"

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

                schema_fields = parse_ods_schema(
                    schema_fields=schema_fields,
                    ods_metadata=table.ods_config.ods_metadata
                )

                assert self.get_schema_migration_sql(table_id, schema_fields) == expected_query 

    def test_should_pick_columns_for_hds_schema_migration(self):
        for table in self.config.tables:
            if table.hds_config:
                if table.hds_config.hds_table_type == HdsTableType.SNAPSHOT:
                    table_id = f"{table.table_name}_HDS_Snapshot"

                elif table.hds_config.hds_table_type == HdsTableType.SCD2:
                    table_id = f"{table.table_name}_HDS_SCD2"

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

                schema_fields = parse_hds_schema(
                    schema_fields=schema_fields,        
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )    

                expected_query = "SELECT `customerID`,`key_id`,`city_name`,`af_metadata_created_at`,`af_metadata_expired_at`,`af_metadata_row_hash` FROM `airflow_test.customer_data_HDS_SCD2`;"

                assert self.get_schema_migration_sql(table_id, schema_fields) == expected_query

    def get_schema_migration_sql(self, table_id, schema_fields):
        migrate_schema = MigrateSchema(
            task_id="schema_migration",
            project_id=self.project_id,
            table_id=table_id,
            dataset_id=self.target_dataset, 
            new_schema_fields=schema_fields
        )

        query, _, _, _= migrate_schema.build_schema_query()

        return query