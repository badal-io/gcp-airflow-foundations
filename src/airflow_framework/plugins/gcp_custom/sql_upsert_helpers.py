def create_truncate_sql(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    columns: list,
    column_mapping: dict,
    ods_metadata: dict
):
    hash_column_name = ods_metadata["hash_column_name"]
    primary_key_hash_column_name = ods_metadata["primary_key_hash_column_name"]
    ingestion_time_column_name = ods_metadata["ingestion_time_column_name"]
    update_time_column_name = ods_metadata["update_time_column_name"]

    columns_str_source: str = ",".join(columns)
    columns_str_target: str = ",".join([column_mapping[i] for i in columns])
    columns_str_keys: str = ",".join(surrogate_keys)

    comma = ","


    return f"""
            TRUNCATE TABLE `{target_dataset}.{target}`;
            INSERT INTO `{target_dataset}.{target}`
            ({columns_str_target}, {ingestion_time_column_name}, {update_time_column_name}, {hash_column_name}, {primary_key_hash_column_name})
            SELECT {columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(TO_JSON_STRING(STRUCT({columns_str_keys})))
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


def create_upsert_sql_with_hash(
    source_dataset: str,
    target_dataset: str,
    source: str,
    target: str,
    surrogate_keys: list,
    update_columns: list,
    columns: list,
    column_mapping: dict,
    ods_metadata: dict
):

    hash_column_name = ods_metadata["hash_column_name"]
    primary_key_hash_column_name = ods_metadata["primary_key_hash_column_name"]
    ingestion_time_column_name = ods_metadata["ingestion_time_column_name"]
    update_time_column_name = ods_metadata["update_time_column_name"]

    columns_str_source: str = ",".join(columns)
    columns_str_target: str = ",".join([column_mapping[i] for i in columns])
    columns_str_keys: str = ",".join(surrogate_keys)

    comma = ","

    return f"""
             MERGE `{target_dataset}.{target}` T
             USING `{source_dataset}.{source}` S
             ON {' AND '.join(
        [f'T.{column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
            WHEN MATCHED THEN UPDATE
                SET {(','.join(f'{column_mapping[col]}=S.{col}' for col in columns )) + f'{comma}' + f'{update_time_column_name}=CURRENT_TIMESTAMP()' + f'{comma}' +  f'{hash_column_name}=TO_BASE64(MD5(TO_JSON_STRING(S)))' }
            WHEN NOT MATCHED THEN
               INSERT ({columns_str_target}, {ingestion_time_column_name}, {update_time_column_name}, {hash_column_name}, {primary_key_hash_column_name})
               VALUES ({columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(TO_JSON_STRING(STRUCT({columns_str_keys}))))
         """