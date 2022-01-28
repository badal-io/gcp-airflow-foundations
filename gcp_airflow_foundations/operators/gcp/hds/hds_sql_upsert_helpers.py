from gcp_airflow_foundations.enums.ingestion_type import IngestionType


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
        column_casting,
        hds_metadata,
        time_partitioning=None,
        gcp_conn_id="google_cloud_default",
    ):

        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.source = source
        self.target = target
        self.surrogate_keys = surrogate_keys
        self.column_mapping = column_mapping
        self.column_casting = column_casting
        self.hds_metadata = hds_metadata
        self.gcp_conn_id = gcp_conn_id
        self.time_partitioning = time_partitioning
        self.columns = columns
        self.hash_column_name = hds_metadata.hash_column_name
        self.eff_start_time_column_name = hds_metadata.eff_start_time_column_name
        self.partition_column_name = hds_metadata.partition_time_column_name
        self.eff_end_time_column_name = hds_metadata.eff_end_time_column_name

        self.columns_str_source: str = ",".join(["`{}`".format(col) for col in columns])
        self.columns_str_keys: str = ",".join(surrogate_keys)
        self.columns_str_target: str = ",".join(
            ["`{}`".format(column_mapping[i]) for i in columns]
        )

    def create_scd2_sql_with_hash(self, ingestion_type):
        TEMPLATE = """
            MERGE `{target}` T
            USING ({source_query}) S
            ON {merge_condition}
            WHEN MATCHED AND {search_condition} THEN {matched_clause}
            WHEN NOT MATCHED BY TARGET THEN {not_matched_clause}
            {not_matched_by_source_clause}
        """

        target = f"{self.target_dataset}.{self.target}"
        source_query = f"""SELECT  {",".join(["{} AS join_key_{}".format(surrogate_key, surrogate_key) for surrogate_key in self.surrogate_keys])}, {",".join([col for col in self.columns])}
            FROM `{self.source_dataset}.{self.source}`
            UNION ALL
            SELECT
                {",".join(["NULL" for col in self.surrogate_keys])},
                {",".join(["source.`{}`".format(col) for col in self.columns])}
                FROM `{self.source_dataset}.{self.source}` source
                JOIN `{self.target_dataset}.{self.target}` target
                ON {' AND '.join([f'target.{self.column_mapping[surrogate_key]}=source.{surrogate_key}' for surrogate_key in self.surrogate_keys])}
                WHERE (
                        ({' OR '.join([f'MD5(TO_JSON_STRING(target.`{self.column_mapping[col]}`)) != MD5(TO_JSON_STRING(source.`{col}`))' for col in self.columns if col not in self.surrogate_keys])})
                        AND target.{self.eff_end_time_column_name} IS NULL
                    )"""

        merge_condition = f"{' AND '.join([f'T.`{self.column_mapping[surrogate_key]}`=S.`join_key_{surrogate_key}`' for surrogate_key in self.surrogate_keys])}"
        search_condition = f"({' OR '.join([f'MD5(TO_JSON_STRING(T.`{self.column_mapping[col]}`)) != MD5(TO_JSON_STRING(S.`{col}`))' for col in self.columns if col not in self.surrogate_keys])})"
        matched_clause = (
            f"UPDATE SET {self.eff_end_time_column_name} = CURRENT_TIMESTAMP()"
        )
        not_matched_clause = f"""INSERT ({self.columns_str_target}, {self.eff_start_time_column_name}, {self.eff_end_time_column_name}, {self.hash_column_name})
            VALUES ({",".join(["`{}`".format(col) for col in self.columns])}, CURRENT_TIMESTAMP(), NULL, TO_BASE64(MD5(TO_JSON_STRING(S))))"""

        if ingestion_type == IngestionType.INCREMENTAL:
            not_matched_by_source_clause = ""

        elif ingestion_type == IngestionType.FULL:
            not_matched_by_source_clause = f"""WHEN NOT MATCHED BY SOURCE THEN UPDATE SET T.{self.eff_end_time_column_name} = CURRENT_TIMESTAMP()"""

        sql = TEMPLATE.format(
            target=target,
            source_query=source_query,
            merge_condition=merge_condition,
            search_condition=search_condition,
            matched_clause=matched_clause,
            not_matched_clause=not_matched_clause,
            not_matched_by_source_clause=not_matched_by_source_clause,
        )

        return sql

    def create_snapshot_sql_with_hash(self, partition_timestamp):

        if self.column_casting:
            COLUMNS = ",".join(
                f"{self.column_casting[col]['function'].format(column=f'`{col}`')} AS `{self.column_mapping[col]}`"
                if col in self.column_casting
                else f"`{col}` AS `{self.column_mapping[col]}`"
                for col in self.columns
            )

        else:
            COLUMNS = ",".join(
                f"`{col}` AS `{self.column_mapping[col]}`" for col in self.columns
            )

        sql = f"""
            SELECT
                {COLUMNS},
                CURRENT_TIMESTAMP() AS {self.eff_start_time_column_name},
                TIMESTAMP_TRUNC('{partition_timestamp}', {self.time_partitioning}) AS {self.partition_column_name},
                TO_BASE64(MD5(TO_JSON_STRING(S))) AS {self.hash_column_name}
            FROM `{self.source_dataset}.{self.source}` S
        """
        return sql
