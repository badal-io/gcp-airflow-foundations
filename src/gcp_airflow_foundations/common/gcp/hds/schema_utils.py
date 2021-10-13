from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType


def parse_hds_schema(
    hds_metadata,
    hds_table_type,
    gcs_schema_object=None,
    schema_fields=None,
    column_mapping=None) -> list:

    source_schema_fields, source_table_columns = read_schema_from_gcs(
        gcs_schema_object=gcs_schema_object,
        schema_fields=schema_fields,
        column_mapping=column_mapping
    )

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

    if hds_table_type == HdsTableType.SCD2:
        source_schema_fields.extend(HDS_EXTRA_FIELDS_SCD2)

    elif hds_table_type == HdsTableType.SNAPSHOT:
        source_schema_fields.extend(HDS_EXTRA_FIELDS_SNAPSHOT)

    return source_schema_fields, source_table_columns    
