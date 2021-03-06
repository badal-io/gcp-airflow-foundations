import logging
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.kms import CloudKMSHook

import gcp_airflow_foundations.common.dataflow.jdbc.mysql.mysql_query_helpers as mysql_helpers
from gcp_airflow_foundations.source_class.jdbc_dataflow_source import (
    JdbcToBQDataflowDagBuilder,
)


class MySQLToBQDataflowDagBuilder(JdbcToBQDataflowDagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """

    source_type = "MYSQL"

    def create_job_params(
        self,
        config_params,
        destination_table,
        table_name,
        destination_schema_table,
        query_schema,
        owner,
        **kwargs,
    ):
        #   1.  Generate SQL Query
        #    a. get source schema from BQ table
        bq_hook = BigQueryHook()
        if not query_schema:
            schema_query = mysql_helpers.get_table_schema_query(
                destination_schema_table, table_name
            )
            schema_df = bq_hook.get_pandas_df(schema_query, dialect="standard")

            column_names = schema_df.iloc[:, 0]
            data_types = schema_df.iloc[:, 1]

            #    b. apply castings if provided
            casted_columns = mysql_helpers.cast_columns(
                column_names, data_types, config_params["sql_casts"]
            )
            logging.info(casted_columns)

            #    c. get query
            query = mysql_helpers.get_query_for_mysql_load_full(
                table_name, casted_columns, owner
            )
            logging.info(query)

        else:
            query = mysql_helpers.get_schema_query(owner)

        # 2. KMS-encrypt credentials for Dataflow job
        logging.info("Encrypting DB credentials with Google Cloud KMS.")

        kms_hook = CloudKMSHook()
        kms_key = config_params["kms_key_path"]

        jdbc_url = bytes(config_params["jdbc_url"], "utf-8")
        jdbc_user = bytes(config_params["jdbc_user"], "utf-8")
        jdbc_pass = bytes(Variable.get(config_params["jdbc_pass_secret_name"]), "utf-8")

        enc_url = kms_hook.encrypt(key_name=kms_key, plaintext=jdbc_url)
        enc_user = kms_hook.encrypt(key_name=kms_key, plaintext=jdbc_user)
        enc_pass = kms_hook.encrypt(key_name=kms_key, plaintext=jdbc_pass)

        # Set up parameters to pass to Dataflow job, and XCom push them
        dataflow_default_options = {
            "project": config_params["project"],
            # "region": config_params["region"],
            "subnetwork": config_params["subnetwork"],
        }
        logging.info(dataflow_default_options)

        ds = kwargs["ds"]

        parameters = {
            "driverClassName": config_params["jdbc_driver_class"],
            "driverJars": config_params["jdbc_jar_path"],
            "KMSEncryptionKey": kms_key,
            "outputTable": destination_table + f"_{ds}",
            "bigQueryLoadingTemporaryDirectory": config_params[
                "bq_load_temp_directory"
            ],
            "connectionURL": enc_url,
            "username": enc_user,
            "password": enc_pass,
            "query": query,
        }
        logging.info(parameters)

        kwargs["ti"].xcom_push(
            key="dataflow_default_options", value=dataflow_default_options
        )
        kwargs["ti"].xcom_push(key="parameters", value=parameters)

    def get_landing_schema(self, schema_table, source_table):

        bq_hook = BigQueryHook()

        schema_query = mysql_helpers.get_table_schema_query(schema_table, source_table)
        schema_df = bq_hook.get_pandas_df(schema_query, dialect="standard")
        schema_df = schema_df.values.tolist()

        column_names = [x[0] for x in schema_df]
        dtypes = [x[1] for x in schema_df]

        dtypes = mysql_helpers.mysql_to_bq(dtypes)

        schema_fields = []
        for i in range(len(dtypes)):
            schema_fields.append(
                {"name": column_names[i], "type": dtypes[i], "mode": "NULLABLE"}
            )

        logging.info(schema_fields)

        return schema_fields
