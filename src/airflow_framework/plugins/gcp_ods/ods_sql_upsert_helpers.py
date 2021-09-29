class SqlHelperODS:
    def __init__(
        self,
        source_dataset,
        target_dataset,
        source,
        target,
        surrogate_keys,
        column_mapping,
        columns,
        ods_metadata,
        gcp_conn_id='google_cloud_default'):

        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.source = source
        self.target = target
        self.surrogate_keys = surrogate_keys
        self.column_mapping = column_mapping
        self.ods_metadata = ods_metadata
        self.gcp_conn_id = gcp_conn_id
        self.columns = columns

        if not column_mapping:
            self.column_mapping = {i:i for i in columns}

        self.hash_column_name = ods_metadata.hash_column_name
        self.primary_key_hash_column_name = ods_metadata.primary_key_hash_column_name
        self.ingestion_time_column_name = ods_metadata.ingestion_time_column_name
        self.update_time_column_name = ods_metadata.update_time_column_name

        self.columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
        self.columns_str_keys: str = ",".join(surrogate_keys)
        self.columns_str_target: str = ",".join(["`{}`".format(column_mapping[i]) for i in columns])


    def create_truncate_sql(self):
        comma = ","

        return f"""
                TRUNCATE TABLE `{self.target_dataset}.{self.target}`;
                INSERT INTO `{self.target_dataset}.{self.target}`
                ({self.columns_str_target}, {self.ingestion_time_column_name}, {self.update_time_column_name}, {self.hash_column_name}, {self.primary_key_hash_column_name})
                SELECT {self.columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(surrogate_key) for surrogate_key in self.surrogate_keys])}], "")))
                FROM `{self.source_dataset}.{self.source}` S
            """


    def create_upsert_sql_with_hash(self):
        comma = ","

        return f"""
                MERGE `{self.target_dataset}.{self.target}` T
                USING `{self.source_dataset}.{self.source}` S
                ON {' AND '.join(
            [f'T.{self.column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in self.surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {(','.join(f'`{self.column_mapping[col]}`=S.`{col}`' for col in self.columns )) + f'{comma}' + f'{self.update_time_column_name}=CURRENT_TIMESTAMP()' + f'{comma}' +  f'{self.hash_column_name}=TO_BASE64(MD5(TO_JSON_STRING(S)))' }
                WHEN NOT MATCHED THEN
                INSERT ({self.columns_str_target}, {self.ingestion_time_column_name}, {self.update_time_column_name}, {self.hash_column_name}, {self.primary_key_hash_column_name})
                VALUES ({self.columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(surrogate_key) for surrogate_key in self.surrogate_keys])}], ""))))
            """
