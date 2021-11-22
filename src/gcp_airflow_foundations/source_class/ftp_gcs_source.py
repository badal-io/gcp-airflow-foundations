from dataclasses import fields
from os import X_OK
from urllib.parse import urlparse
from abc import ABC, abstractmethod, abstractproperty
import logging
import json
import pandas as pd
from dacite import from_dict
from dataclasses import dataclass

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from gcp_airflow_foundations.base_class.gcs_table_config import GCSTableConfig
from gcp_airflow_foundations.operators.api.operators.sf_to_gcs_query_operator import SalesforceToGcsQueryOperator
from gcp_airflow_foundations.operators.api.sensors.gcs_sensor import GCSObjectListExistenceSensor
from gcp_airflow_foundations.source_class.ftp_source import FTPtoBQDagBuilder
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder


class GCSFiletoBQDagBuilder(FTPtoBQDagBuilder):
    """
    Builds DAGs to load files from GCS to a BigQuery Table.

    For GCS->BQ ingestion, a metadata file is required. It can be a fixed file, or can be a new file supplied daily.
    Airflow context variables are supported for the file naming, e.g.
        TABLE_METADATA_FILE_{{ ds }}.csv
    for a metadata file supplied daily.

    The format of the metadata file should be a csv with one column as follows:
        FILE_NAME_1
        ...
        FILE_NAME_N
    with all files to ingest
    """
    source_type = "GCS"
    gcs_hook = GCSHook()

    def metadata_file_sensor(self, table_config, taskgroup):
        """
        Implements a sensor for the metadata file specified in the table config.
        """
        metadata_file_name = table_config.extra_options.get("gcs_table_config")["metadata_file"]
        bucket = self.config.source.extra_options["gcs_bucket"]

        return GCSObjectExistenceSensor(
            task_id="wait_for_metadata_file",
            bucket=bucket,
            object=metadata_file_name,
            task_group=taskgroup
        )

    def file_ingestion_task(self, table_config, taskgroup):
        """
        No ingestion is needed - data is already in GCS, so return a dummy operator.
        """
        return DummyOperator(
            task_id="dummy_file_ingestion_operator",
            task_group=taskgroup
        )

    def schema_file_sensor(self, table_config, taskgroup):
        """
        Implements an Airflow sensor to wait for an (optional) schema file in GCS
        """
        bucket = self.config.source.extra_options["gcs_bucket"]
        schema_file_name = None
        if "schema_file" in table_config.extra_options.get("gcs_table_config"):
            schema_file_name = table_config.extra_options.get("gcs_table_config")["schema_file"]
        logging.info(schema_file_name)

        if schema_file_name:
            return GCSObjectExistenceSensor(
                task_id="wait_for_schema_file",
                bucket=bucket,
                object=schema_file_name,
                task_group=taskgroup
            )
        else:
            return DummyOperator(
                task_id="dummy_schema_file_ingestion_operator",
                task_group=taskgroup
            )

    def get_file_list_task(self, table_config, taskgroup):
        return PythonOperator(
            task_id="get_file_list",
            op_kwargs={"table_config": table_config},
            python_callable=self.get_list_of_files,
            task_group=taskgroup
        )

    def file_sensor(self, table_config, taskgroup):
        """
        Returns an Airflow sensor that waits for the list of files specified by the metadata file provided.
        """
        bucket = self.config.source.extra_options["gcs_bucket"]
        metadata_file_name = table_config.extra_options.get("gcs_table_config")["metadata_file"]

        files_to_wait_for = "{{ ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list') }}"

        return GCSObjectListExistenceSensor(
            task_id="wait_for_files_to_ingest",
            bucket=bucket,
            objects=files_to_wait_for,
            task_group=taskgroup
        )

    def load_to_landing_task(self, table_config, taskgroup):
        return PythonOperator(
            task_id="load_gcs_to_landing_zone",
            op_kwargs={"table_config": table_config},
            python_callable=self.load_to_landing_py_op_task,
            task_group=taskgroup
        )

    def load_to_landing_py_op_task(self, table_config, **kwargs):
        data_source = self.config.source

        ti = kwargs['ti']
        ds = kwargs['ds']

        files_to_load = ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list')
        logging.info(files_to_load)

        # Parameters
        bucket = self.config.source.extra_options["gcs_bucket"]
        source_format = table_config.extra_options.get("gcs_table_config")["source_format"]
        gcp_project = data_source.gcp_project
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset 
        destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}" + f"_{ds}"

        schema_file_name = None
        if "schema_file" in table_config.extra_options.get("gcs_table_config"):
            schema_file_name = table_config.extra_options.get("gcs_table_config")["schema_file"]

        # Get files to load from metadata file
        if schema_file_name:
            schema_file = self.gcs_hook.download(bucket_name=bucket, object_name=schema_file_name)

            # Only supports json schema file format - add additional support if required
            schema_fields = json.loads(schema_file)
            logging.info(schema_fields)
            gcs_to_bq = GCSToBigQueryOperator(
                task_id='import_files_to_bq_landing',
                bucket=bucket,
                source_objects=files_to_load,
                source_format=source_format,
                schema_fields=schema_fields,
                destination_project_dataset_table=destination_table,
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=1,
            )
        else:
            gcs_to_bq = GCSToBigQueryOperator(
                task_id='import_files_to_bq_landing',
                bucket=bucket,
                source_objects=files_to_load,
                source_format=source_format,
                destination_project_dataset_table=destination_table,
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=1,
            )
        gcs_to_bq.execute(context=kwargs)

    def get_list_of_files(self, table_config, **kwargs):
        # XCom push the list of files
        bucket = self.config.source.extra_options["gcs_bucket"]
        metadata_file_name = table_config.extra_options.get("gcs_table_config")["metadata_file"]

        metadata_file = self.gcs_hook.download(bucket_name=bucket, object_name=metadata_file_name, filename="metadata.csv")
        file_list = []
        with open('metadata.csv', newline='') as f:
            for line in f:
                file_list.append(line.strip())

        # support replacing files with current date
        ds = kwargs["ds"]
        file_list[:] = [file.replace("{{ ds }}", ds) if "{{ ds }}" in file else file for file in file_list]

        kwargs['ti'].xcom_push(key='file_list', value=file_list)

    def validate_extra_options(self):
        tables = self.config.tables
        for table in tables:
            gcs_table_cfg = from_dict(data_class=GCSTableConfig, data=table.extra_options.get("gcs_table_config"))