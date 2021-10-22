from gcp_airflow_foundations.enums.hds_table_type import HdsTableType

def parse_hds_schema(
    hds_metadata,
    hds_table_type,
    schema_fields) -> list:

    HDS_EXTRA_FIELDS_SCD2 = [
        {
            "name": hds_metadata.eff_start_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": hds_metadata.eff_end_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": hds_metadata.hash_column_name,
            "type": "STRING"
        }
    ]

    HDS_EXTRA_FIELDS_SNAPSHOT = [
        {
            "name": hds_metadata.eff_start_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": hds_metadata.partition_time_column_name,
            "type": "TIMESTAMP"
        },
        {
            "name": hds_metadata.hash_column_name,
            "type": "STRING"
        }
    ]

    hds_schema_fields = []

    hds_schema_fields.extend(schema_fields)

    if hds_table_type == HdsTableType.SCD2:
        hds_schema_fields.extend(HDS_EXTRA_FIELDS_SCD2)

    elif hds_table_type == HdsTableType.SNAPSHOT:
        hds_schema_fields.extend(HDS_EXTRA_FIELDS_SNAPSHOT)

    return hds_schema_fields
