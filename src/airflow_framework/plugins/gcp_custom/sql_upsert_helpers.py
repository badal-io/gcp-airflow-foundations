from airflow_framework.base_class.ods_metadata_config import OdsTableMetadataConfig
from airflow_framework.base_class.hds_metadata_config import HdsTableMetadataConfig

def create_truncate_sql(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    columns: list,
    column_mapping: dict,
    ods_metadata: OdsTableMetadataConfig
):
    hash_column_name = ods_metadata.hash_column_name
    primary_key_hash_column_name = ods_metadata.primary_key_hash_column_name
    ingestion_time_column_name = ods_metadata.ingestion_time_column_name
    update_time_column_name = ods_metadata.update_time_column_name

    columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
    columns_str_keys: str = ",".join(surrogate_keys)

    comma = ","

    if column_mapping:
        columns_str_target: str = ",".join(["`{}`".format(column_mapping[i]) for i in columns])
    else:
        columns_str_target = columns_str_source    

    return f"""
            TRUNCATE TABLE `{target_dataset}.{target}`;
            INSERT INTO `{target_dataset}.{target}`
            ({columns_str_target}, {ingestion_time_column_name}, {update_time_column_name}, {hash_column_name}, {primary_key_hash_column_name})
            SELECT {columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(TO_JSON_STRING(STRUCT({columns_str_keys}))))
            FROM `{source_dataset}.{source}` S
        """

def create_upsert_sql(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    column_mapping: dict
):
    if column_mapping:
        return f"""
                MERGE `{target_dataset}.{target}` T
                USING `{source_dataset}.{source}` S
                ON {' AND '.join(
            [f'T.{column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {','.join(f'{column_mapping[col]}=S.{col}' for col in update_columns ) }
                WHEN NOT MATCHED THEN
                    INSERT ROW
            """
    else:
        return f"""
                MERGE `{target_dataset}.{target}` T
                USING `{source_dataset}.{source}` S
                ON {' AND '.join(
            [f'T.{surrogate_key}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {','.join(f'{col}=S.{col}' for col in update_columns ) }
                WHEN NOT MATCHED THEN
                    INSERT ROW
            """


def create_upsert_sql_with_hash(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    columns: list,
    column_mapping: dict,
    ods_metadata: HdsTableMetadataConfig
):

    hash_column_name = ods_metadata.hash_column_name
    primary_key_hash_column_name = ods_metadata.primary_key_hash_column_name
    ingestion_time_column_name = ods_metadata.ingestion_time_column_name
    update_time_column_name = ods_metadata.update_time_column_name

    columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
    columns_str_keys: str = ",".join(surrogate_keys)

    comma = ","

    if column_mapping:
        columns_str_target: str = ",".join(["`{}`".format(column_mapping[i]) for i in columns])
        return f"""
                MERGE `{target_dataset}.{target}` T
                USING `{source_dataset}.{source}` S
                ON {' AND '.join(
            [f'T.{column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {(','.join(f'`{column_mapping[col]}`=S.`{col}`' for col in columns )) + f'{comma}' + f'{update_time_column_name}=CURRENT_TIMESTAMP()' + f'{comma}' +  f'{hash_column_name}=TO_BASE64(MD5(TO_JSON_STRING(S)))' }
                WHEN NOT MATCHED THEN
                INSERT ({columns_str_target}, {ingestion_time_column_name}, {update_time_column_name}, {hash_column_name}, {primary_key_hash_column_name})
                VALUES ({columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(TO_JSON_STRING(STRUCT({columns_str_keys})))))
            """
    else:
        columns_str_target = columns_str_source
        return f"""
                MERGE `{target_dataset}.{target}` T
                USING `{source_dataset}.{source}` S
                ON {' AND '.join(
            [f'T.{surrogate_key}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {(','.join(f'`{col}`=S.`{col}`' for col in columns )) + f'{comma}' + f'{update_time_column_name}=CURRENT_TIMESTAMP()' + f'{comma}' +  f'{hash_column_name}=TO_BASE64(MD5(TO_JSON_STRING(S)))' }
                WHEN NOT MATCHED THEN
                INSERT ({columns_str_target}, {ingestion_time_column_name}, {update_time_column_name}, {hash_column_name}, {primary_key_hash_column_name})
                VALUES ({columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(TO_JSON_STRING(STRUCT({columns_str_keys})))))
            """

def create_scd2_sql_with_hash(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    columns: list,
    column_mapping: dict,
    hds_metadata: OdsTableMetadataConfig
):

    columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
    columns_str_keys: str = ",".join(surrogate_keys)

    comma = ","
    if column_mapping:
        columns_str_target: str = ",".join(["`{}`".format(column_mapping[i]) for i in columns])

        return f"""
                MERGE `{target_dataset}.{target}` T
                USING (
                    SELECT  {",".join(["{} AS join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in surrogate_keys])}, * 
                    FROM `{source_dataset}.{source}`
                    UNION ALL 
                    SELECT
                        {",".join(["source.`{}`".format(surrogate_key) for surrogate_key in surrogate_keys])},
                        {",".join(["NULL" for surrogate_key in surrogate_keys])},
                        {",".join(["source.`{}`".format(col) for col in columns if col not in surrogate_keys])}
                        FROM `{source_dataset}.{source}` source
                        JOIN `{target_dataset}.{target}` target
                        ON {' AND '.join([f'target.{column_mapping[surrogate_key]}=source.{surrogate_key}' for surrogate_key in surrogate_keys])}
                        WHERE ( 
                                    MD5(ARRAY_TO_STRING([{",".join(["CAST(target.`{}` AS STRING)".format(column_mapping[i]) for i in columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(source.`{}` AS STRING)".format(col) for col in columns])}], ""))
                                AND target.eff_end_time IS NULL)) S
                ON {' AND '.join([f'T.{column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED AND MD5(ARRAY_TO_STRING([{",".join(["CAST(T.`{}` AS STRING)".format(column_mapping[i]) for i in columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(col) for col in columns])}], "")) THEN UPDATE
                    SET eff_end_time = CURRENT_TIMESTAMP(), is_current = FALSE
                WHEN NOT MATCHED THEN
                    INSERT ({columns_str_target}, eff_start_time, eff_end_time, is_current)
                    VALUES ({",".join(["join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in surrogate_keys])},{",".join(["`{}`".format(col) for col in columns if col not in surrogate_keys])}, CURRENT_TIMESTAMP(), NULL, TRUE)
                """
    else:
        columns_str_target = columns_str_source

        return f"""
                MERGE `{target_dataset}.{target}` T
                USING (
                    SELECT  {",".join(["{} AS join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in surrogate_keys])}, * 
                    FROM `{source_dataset}.{source}`
                    UNION ALL 
                    SELECT
                        {",".join(["source.`{}`".format(surrogate_key) for surrogate_key in surrogate_keys])},
                        {",".join(["NULL" for surrogate_key in surrogate_keys])},
                        {",".join(["source.`{}`".format(col) for col in columns if col not in surrogate_keys])}
                        FROM `{source_dataset}.{source}` source
                        JOIN `{target_dataset}.{target}` target
                        ON {' AND '.join([f'target.{surrogate_key}=source.{surrogate_key}' for surrogate_key in surrogate_keys])}
                        WHERE ( 
                                    MD5(ARRAY_TO_STRING([{",".join(["CAST(target.`{}` AS STRING)".format(i) for i in columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(source.`{}` AS STRING)".format(col) for col in columns])}], ""))
                                AND target.eff_end_time IS NULL)) S
                ON {' AND '.join([f'T.{surrogate_key}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
                WHEN MATCHED AND MD5(ARRAY_TO_STRING([{",".join(["CAST(T.`{}` AS STRING)".format(i) for i in columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(col) for col in columns])}], "")) THEN UPDATE
                    SET eff_end_time = CURRENT_TIMESTAMP(), is_current = FALSE
                WHEN NOT MATCHED THEN
                    INSERT ({columns_str_target}, eff_start_time, eff_end_time, is_current)
                    VALUES ({",".join(["join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in surrogate_keys])},{",".join(["`{}`".format(col) for col in columns if col not in surrogate_keys])}, CURRENT_TIMESTAMP(), NULL, TRUE)
                """
                