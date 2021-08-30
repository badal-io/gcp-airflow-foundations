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
    column_mapping: dict
):
    columns_str_source: str = ",".join(columns)
    columns_str_target: str = ",".join([column_mapping[i] for i in columns])

    comma = ","
    if (update_columns == []):
        comma = ""

    return f"""
             MERGE `{target_dataset}.{target}` T
             USING `{source_dataset}.{source}` S
             ON {' AND '.join(
        [f'T.{column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in surrogate_keys])}
            WHEN MATCHED THEN UPDATE
                SET {(','.join(f'{column_mapping[col]}=S.{col}' for col in update_columns )) + f'{comma}' + 'bq_updated_at=CURRENT_TIMESTAMP()' }
            WHEN NOT MATCHED THEN
               INSERT ({columns_str_target}, bq_inserted_at, bq_updated_at, cdc_hash)
               VALUES ({columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))))
         """