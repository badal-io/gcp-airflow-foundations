def parse_ods_schema(
    ods_metadata,
    schema_fields) -> list:

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

    ods_schema_fields = []

    ods_schema_fields.extend(schema_fields)
    ods_schema_fields.extend(ODS_EXTRA_FIELDS)

    return ods_schema_fields    
