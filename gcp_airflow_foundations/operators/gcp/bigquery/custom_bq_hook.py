import pandas as pd
import json
import pyarrow.parquet as pq
import pyarrow

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.decorators import apply_defaults

from google.cloud import bigquery


@apply_defaults
class CustomBigQueryHook(BigQueryHook):
    """
    Extends the default BigQuery Hook with custom methods to interact with Google Cloud BigQuery.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :type gcp_conn_id: Optional[str]
    """
    def __init__(
        self,
        gcp_conn_id: str,
        **kwargs
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            **kwargs
        )

    def load_nested_table_from_dataframe(
        self,
        df: pd.DataFrame,
        project_id: str,
        destination_dataset_table: str,
        write_disposition: str = 'WRITE_TRUNCATE'
    ) -> bigquery.job.LoadJob:
        """
        Loads a pandas.DataFrame to a BigQuery table. It supports nested fields.

        Returns a BigQuety LoadJob object.

        :param df: The Dataframe to load in BigQuery
        :type df: pandas.DataFrame
        :param project_id: The Google Cloud Project ID
        :type project_id: str
        :param destination_dataset_table: The full table name to load the data. Format: (<project>.|<project>:)<dataset>.<table>
        :type destination_dataset_table: str
        :param write_disposition: Specifies the action that occurs if the destination table already exists. (default: 'WRITE_TRUNCATE')
        :type write_disposition: str
        """

        client = self.get_client(project_id=project_id)

        writer = pyarrow.BufferOutputStream()
        pq.write_table(
            pyarrow.Table.from_pandas(df),
            writer,
            use_compliant_nested_type=True
        )
        reader = pyarrow.BufferReader(writer.getvalue())

        parquet_options = bigquery.format_options.ParquetOptions()
        parquet_options.enable_list_inference = True

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.parquet_options = parquet_options
        job_config.write_disposition = write_disposition

        return client.load_table_from_file(
            reader, destination_dataset_table, job_config=job_config
        )     