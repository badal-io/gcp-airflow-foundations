from dataclasses import fields
from urllib.parse import urlparse
from abc import ABC, abstractmethod, abstractproperty
import logging

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_airflow_foundations.base_class.salesforce_ingestion_config import SalesforceIngestionConfig

from gcp_airflow_foundations.operators.api.operators.sf_to_gcs_query_operator import SalesforceToGcsQueryOperator
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder


class FTPtoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load files from FTP to BigQuery.
    """
    source_type = "FTP"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type     

    def get_bq_ingestion_task(self, dag, table_config):
        taskgroup = TaskGroup(group_id="ftp_taskgroup")

        metadata_sensor_task = self.metadata_file_sensor(table_config, taskgroup)
        schema_sensor_task = self.schema_file_sensor(table_config, taskgroup)
        get_file_list_task = self.get_file_list_task(table_config, taskgroup)
        sensor_task = self.file_sensor(table_config, taskgroup)
        file_ingestion_task = self.file_ingestion_task(table_config, taskgroup)
        load_to_landing_task = self.load_to_landing_task(table_config, taskgroup)

        metadata_sensor_task >> schema_sensor_task >> get_file_list_task >> sensor_task >> file_ingestion_task >> load_to_landing_task

        return taskgroup

    @abstractmethod
    def metadata_file_sensor(self, table_config):
        """
        Implements an Airflow sensor to wait for initial metadata files for ingestion,
        e.g. a metadata file containing a list of files to ingest for .csv files from GCS->BQ,
             .SUCCESS file for a directory of .parquet files from an SFTP server, etc.
        """
        pass

    @abstractmethod
    def schema_file_sensor(self, table_config):
        """
        Implements an Airflow sensor to wait for an (optional) schema file for the table
        """
        pass

    @abstractmethod
    def get_file_list_task(self, table_config):
        """
        Implements a Python operator that should get a list of the files to ingest and xcom push them as follows:
            kwargs['ti'].xcom_push(key='file_list', value=file_list)
        """
        pass

    @abstractmethod
    def file_ingestion_task(self, table_config):
        """
        Implements an Airflow task to ingest the files from the FTP source into GCS (e.g. from an SFTP server or an AWS bucket)
        """
        pass

    @abstractmethod
    def file_sensor(self, table_config):
        """
        Returns an Airflow sensor that waits for the list of files specified the metadata file provided
        Should be Xcom pulled from get_file_list_task()
        """
        pass

    @abstractmethod
    def load_to_landing_task(self, table_config):
        pass

    def validate_extra_options(self):
        tables = self.config.tables