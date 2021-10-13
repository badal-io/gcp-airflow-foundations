class SqlHelperHDS:
    """
    SQL helper class used to formulate the SQL queries for the merge operations of HDS tables.
    
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
    :param hds_metadata: User-provided options for HDS metadata column naming
    :type hds_metadata: HdsTableMetadataConfig
    """

    def __init__(
        self,
        source_dataset,
        target_dataset,
        source,
        target,
        columns,
        surrogate_keys,
        column_mapping,
        hds_metadata,
        time_partitioning=None,
        gcp_conn_id='google_cloud_default'):

        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.source = source
        self.target = target
        self.surrogate_keys = surrogate_keys
        self.column_mapping = column_mapping
        self.hds_metadata = hds_metadata
        self.gcp_conn_id = gcp_conn_id
        self.time_partitioning = time_partitioning
        self.columns = columns

        if not column_mapping:
            self.column_mapping = {i:i for i in columns}
        else:
            for i in columns:
                if i not in column_mapping:
                    column_mapping[i] = i

        self.hash_column_name = hds_metadata.hash_column_name
        self.eff_start_time_column_name = hds_metadata.eff_start_time_column_name
        self.partition_column_name = hds_metadata.partition_time_column_name
        self.eff_end_time_column_name = hds_metadata.eff_end_time_column_name

        self.columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
        self.columns_str_keys: str = ",".join(surrogate_keys)
        self.columns_str_target: str = ",".join(["`{}`".format(column_mapping[i]) for i in columns])

    def create_scd2_sql_with_hash(self):
        comma = ","

        TEMPLATE = """
                    MERGE `{target}` T
                    USING ( {source_query} ) S
                    ON {merge_condition}
                    WHEN MATCHED AND {search_condition} THEN {matched_clause}
                    WHEN NOT MATCHED THEN {not_matched_clause}
        """

        target = f"{self.target_dataset}.{self.target}"
        source_query = f"""
                SELECT  {",".join(["{} AS join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in self.surrogate_keys])}, * 
                FROM `{self.source_dataset}.{self.source}`
                UNION ALL 
                SELECT
                    {",".join(["source.`{}`".format(surrogate_key) for surrogate_key in self.surrogate_keys])},
                    {",".join(["NULL" for surrogate_key in self.surrogate_keys])},
                    {",".join(["source.`{}`".format(col) for col in self.columns if col not in self.surrogate_keys])}
                    FROM `{self.source_dataset}.{self.source}` source
                    JOIN `{self.target_dataset}.{self.target}` target
                    ON {' AND '.join([f'target.{self.column_mapping[surrogate_key]}=source.{surrogate_key}' for surrogate_key in self.surrogate_keys])}
                    WHERE ( 
                                MD5(ARRAY_TO_STRING([{",".join(["CAST(target.`{}` AS STRING)".format(self.column_mapping[i]) for i in self.columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(source.`{}` AS STRING)".format(col) for col in self.columns])}], ""))
                            AND target.{self.eff_end_time_column_name} IS NULL)
        """
        merge_condition = f"{' AND '.join([f'T.{self.column_mapping[surrogate_key]}=S.{surrogate_key}' for surrogate_key in self.surrogate_keys])}"
        search_condition = f"""MD5(ARRAY_TO_STRING([{",".join(["CAST(T.`{}` AS STRING)".format(self.column_mapping[i]) for i in self.columns])}], "")) != MD5(ARRAY_TO_STRING([{",".join(["CAST(S.`{}` AS STRING)".format(col) for col in self.columns])}], ""))"""
        matched_clause = f"UPDATE SET {self.eff_end_time_column_name} = CURRENT_TIMESTAMP()"
        not_matched_clause = f"""
            INSERT ({self.columns_str_target}, {self.eff_start_time_column_name}, {self.eff_end_time_column_name}, {self.hash_column_name})
            VALUES ({",".join(["join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in self.surrogate_keys])},{",".join(["`{}`".format(col) for col in self.columns if col not in self.surrogate_keys])}, CURRENT_TIMESTAMP(), NULL, TO_BASE64(MD5(TO_JSON_STRING(S))))
        """

        sql = TEMPLATE.format(
            target=target,
            source_query=source_query,
            merge_condition=merge_condition,
            search_condition=search_condition,
            matched_clause=matched_clause,
            not_matched_clause=not_matched_clause
        )

        return sql


    def create_snapshot_sql_with_hash(self):
        comma = ","

        return f"""
                INSERT INTO `{self.target_dataset}.{self.target}`
                ({self.columns_str_target}, {self.eff_start_time_column_name}, {self.partition_column_name}, {self.hash_column_name})
                SELECT {self.columns_str_source}, CURRENT_TIMESTAMP(), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {self.time_partitioning}), TO_BASE64(MD5(TO_JSON_STRING(S)))
                FROM `{self.source_dataset}.{self.source}` S
            """