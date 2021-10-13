from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs


def parse_ods_schema(
    ods_metadata,
    gcs_schema_object=None,
    schema_fields=None,
    column_mapping=None) -> list:

    source_schema_fields, source_table_columns = read_schema_from_gcs(
        gcs_schema_object=gcs_schema_object,
        schema_fields=schema_fields,
        column_mapping=column_mapping
    )

    ODS_EXTRA_FIELDS = [
        {
            "name": ods_metadata.ingestion_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": ods_metadata.update_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": ods_metadata.primary_key_hash_column_name,
            "type": "STRING"
        },
        {
            "name": ods_metadata.hash_column_name,
            "type": "STRING"
        }
    ]

    source_schema_fields.extend(ODS_EXTRA_FIELDS)

    return source_schema_fields, source_table_columns    
