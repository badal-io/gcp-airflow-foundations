class SqlHelperODS:
    """
    SQL helper class used to formulate the SQL queries for the merge operations of ODS tables.
    
    :param source: Source table name
    :type source: str
    :param target: Target table name
    :type target: str
    :param source_dataset: Source dataset name
    :type source_dataset: str
    :param target_dataset: Target dataset name
    :type target_dataset: str
    :param surrogate_keys: Surrogate keys of the target table
    :type surrogate_keys: list
    :param columns: List of source table columns
    :type columns: list
    :param gcp_conn_id: Airflow GCP connection ID
    :type gcp_conn_id: str
    :param column_mapping: Column mapping
    :type column_mapping: dict
    :param time_partitioning: Time partitioning option for BigQuery target table. One of HOUR, DAY, or MONTH
    :type time_partitioning: str
    :param ods_metadata: User-provided options for ODS metadata column naming
    :type ods_metadata: OdsTableMetadataConfig
    """

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
        self.hash_column_name = ods_metadata.hash_column_name
        self.primary_key_hash_column_name = ods_metadata.primary_key_hash_column_name
        self.ingestion_time_column_name = ods_metadata.ingestion_time_column_name
        self.update_time_column_name = ods_metadata.update_time_column_name

        self.columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
        self.columns_str_keys: str = ",".join(surrogate_keys)
        self.columns_str_target: str = ",".join(["`{}`".format(self.column_mapping[i]) for i in columns])

    def create_insert_sql(self):
        rows = f"""{self.columns_str_target}, {self.ingestion_time_column_name}, {self.update_time_column_name}, {self.hash_column_name}, {self.primary_key_hash_column_name}"""
        values = f"""{self.columns_str_source}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TO_BASE64(MD5(TO_JSON_STRING(S))), TO_BASE64(MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(surrogate_key) for surrogate_key in self.surrogate_keys])}], "")))"""
        return rows, values

    def create_full_sql(self):
        rows, values = self.create_insert_sql()

        return f"""
            SELECT {",".join([f"`{i}` AS `{self.column_mapping[i]}`" for i in self.columns])},
                CURRENT_TIMESTAMP() AS {self.ingestion_time_column_name},
                CURRENT_TIMESTAMP() AS {self.update_time_column_name}, 
                TO_BASE64(MD5(TO_JSON_STRING(S))) AS {self.hash_column_name}, 
                TO_BASE64(MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(surrogate_key) for surrogate_key in self.surrogate_keys])}], ""))) AS {self.primary_key_hash_column_name}
            FROM `{self.source_dataset}.{self.source}` S
        """

    def create_upsert_sql_with_hash(self):
        comma = ","

        rows, values = self.create_insert_sql()

        return f"""
                MERGE `{self.target_dataset}.{self.target}` T
                USING `{self.source_dataset}.{self.source}` S
                ON {' AND '.join(
            [f'T.{self.column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in self.surrogate_keys])}
                WHEN MATCHED THEN UPDATE
                    SET {(','.join(f'`{self.column_mapping[col]}`=S.`{col}`' for col in self.columns )) + f'{comma}' + f'{self.update_time_column_name}=CURRENT_TIMESTAMP()' + f'{comma}' +  f'{self.hash_column_name}=TO_BASE64(MD5(TO_JSON_STRING(S)))' }
                WHEN NOT MATCHED THEN
                    INSERT ({rows})
                    VALUES ({values})
            """
