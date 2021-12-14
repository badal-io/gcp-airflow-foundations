from dataclasses import fields
from os import X_OK
from urllib.parse import urlparse
from abc import ABC, abstractmethod, abstractproperty
import logging
import json
import pandas as pd
from dacite import from_dict
from dataclasses import dataclass
from datetime import datetime
import os
import glob
import shutil

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.sftp.hooks import SFTPHook
from airflow.exceptions import AirflowException
from airflow.sensors.python import PythonSensor

from gcp_airflow_foundations.common.sftp.sftp_helpers import SSHSession
from gcp_airflow_foundations.base_class.gcs_table_config import GCSTableConfig
from gcp_airflow_foundations.base_class.gcs_source_config import GCSSourceConfig
from gcp_airflow_foundations.operators.api.operators.sf_to_gcs_query_operator import SalesforceToGcsQueryOperator
from gcp_airflow_foundations.operators.api.sensors.gcs_sensor import GCSObjectListExistenceSensor
from gcp_airflow_foundations.operators.api.sensors.gcs_prefix_sensor import GCSObjectPrefixListExistenceSensor
from gcp_airflow_foundations.source_class.ftp_sftp_source import SFTPFiletoBQDagBuilder
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder
from google.cloud import bigquery


class PlumeFiletoBQDagBuilder(SFTPFiletoBQDagBuilder):
    """
    Builds DAGs to load files from GCS to a BigQuery Table.

    For GCS->BQ ingestion, either a metadata file is required or the field templated_file_name must be provided. 
    If a metadata file is provided, itt can be a fixed file, or can be a new file supplied daily.
    Airflow context variables are supported for the file naming, e.g.
        TABLE_METADATA_FILE_{{ ds }}.csv
    for a metadata file supplied daily.

    The format of the metadata file should be a csv with one column as follows:
        FILE_NAME_1
        ...
        FILE_NAME_N
    with all files to ingest
    """
    source_type = "PLUME"

    def get_file_list_task(self, table_config, taskgroup):
        if "metadata_file" in table_config.extra_options.get("gcs_table_config"):
            execution_type = "metadata"
        else:
            execution_type = "success"

        return PythonOperator(
            task_id="get_file_list",
            op_kwargs={"table_config": table_config,
                        "execution_type": execution_type},
            python_callable=self.get_list_of_files,
            task_group=taskgroup
        )

    @abstractmethod
    def file_sensor(self, table_config, taskgroup):
        """
        Returns an Airflow sensor that waits for the list of files specified by the metadata file provided.
        """
        #bucket = self.config.source.extra_options["gcs_bucket"]
        #files_to_wait_for = "{{ ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list') }}"
        pass


    def load_to_landing_task(self, table_config, taskgroup):
        return PythonOperator(
            task_id="load_gcs_to_landing_zone",
            op_kwargs={"table_config": table_config},
            python_callable=self.load_to_landing_plume_task,
            task_group=taskgroup
        )   

    def load_to_landing_plume_task(self, table_config, **kwargs):
        return DummyOperator(task_id="eh")


    def get_list_of_files(self, table_config, **kwargs):
        # XCom push the list of files
        if "dir_upload" in table_config.extra_options.get("sftp_table_config"):
            file_list = [table_config.extra_options.get("sftp_table_config")["dir_upload"]]
        else:
            bucket = self.config.source.extra_options["gcs_bucket"]
            if "metadata_file" in table_config.extra_options.get("sftp_table_config"):
                metadata_file_name = table_config.extra_options.get("sftp_table_config")["metadata_file"]
                metadata_file = self.gcs_hook.download(bucket_name=bucket, object_name=metadata_file_name, filename="metadata.csv")
                file_list = []
                with open('metadata.csv', newline='') as f:
                    for line in f:
                        file_list.append(line.strip())
            else:
                templated_file_name = self.config.source.extra_options["sftp_source_config"]["templated_file_name"]
                templated_file_name = templated_file_name.replace("{{ TABLE_NAME }}", table_config.table_name)
                file_list = [templated_file_name]

            # support replacing files with current dates
            ds = kwargs["ds"]
            ds = datetime.strptime(ds, "%Y-%m-%d").strftime(self.config.source.extra_options["gcs_source_config"]["date_format"])
            file_list[:] = [file.replace("{{ ds }}", ds) if "{{ ds }}" in file else file for file in file_list]

            # add dir prefix to files
            x = "directory_prefix" in self.config.source.extra_options["sftp_source_config"]
            logging.info(x)
            if "directory_prefix" in self.config.source.extra_options["sftp_source_config"]:
                dir_prefix = self.config.source.extra_options["sftp_source_config"]["directory_prefix"]
                file_list[:] = [dir_prefix + file for file in file_list]
            logging.info(file_list)

        kwargs['ti'].xcom_push(key='file_list', value=file_list)
    
    @abstractmethod
    def get_sftp_sensor(self, table_config, taskgroup):
        return PythonSensor(
            task_id="sftp_sensor",
            op_kwargs={"table_config": table_config},
            python_callable=self.sftp_flag_sensor,
            task_group=taskgroup
        )
    
    @abstractmethod
    def get_sftp_ingestion_operator(self, table_config, taskgroup):
        return PythonOperator(
            task_id="load_sftp_to_gcs",
            op_kwargs={"table_config": table_config},
            python_callable=self.load_sftp_to_gcs,
            task_group=taskgroup
        )

    def sftp_flag_sensor(self, table_config, dir_prefix, flag_file, **kwargs)
        sftp_hook = SFTPHook()

        data_source = self.config.source
        flag_file_dataset = data_source.extra_options["gcs_source_config"]["flag_file_dataset"]
        bucket = self.config.source.extra_options["gcs_bucket"]
        source_format = self.config.source.extra_options["gcs_source_config"]["source_format"]
        field_delimeter = self.config.source.extra_options["gcs_source_config"]["delimeter"]
        gcp_project = data_source.gcp_project
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset 
        destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}" + f"_{ds}"
        table_name = table_config.landing_zone_table_name_override

        bq_hook = BigQueryHook()
        bq_client = bigquery.Client(project=gcp_project,
                                    credentials=bq_hook._get_credentials())

        flag_file_path = f"{dir_prefix}/{flag_file}"
        mtime = sftp_hook.get_stats(flag_file_path).st_mtime
        formatted_mtime = datetime.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S")
        mtime = int(mtime)

        query = f"""SELECT LAST_UPDATED_UNIX from {gcp_project}.{flag_file_dataset}.SUCCESS_TABLE where TABLE_NAME = "{table_name}" ORDER BY LAST_UPDATED_UNIX DESC LIMIT 1 """

        timestamp = None
        query_results = bq_client.query(query).to_dataframe().values.tolist()
        logging.info(query_results)
        if query_results:
            timestamp = query_results[0][0]

        logging.info(timestamp)

        TO_LOAD = False
        
        if not timestamp or mtime > timestamp:
            # Load new timestamp
            vals = {"TABLE_NAME": table_name, "LAST_UPDATED": formatted_mtime, "LAST_UPDATED_UNIX": mtime}

            schema = [bigquery.SchemaField("TABLE_NAME", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("LAST_UPDATED", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("LAST_UPDATED_UNIX", "INTEGER", mode="NULLABLE")]

            job_config = bigquery.LoadJobConfig(
                source_format="NEWLINE_DELIMITED_JSON",
                schema=schema
            )

            job = bq_client.load_table_from_json([vals], f"{gcp_project}.{flag_file_dataset}.SUCCESS_TABLE", job_config=job_config)

            TO_LOAD = True
        
        return TO_LOAD

    def load_sftp_to_gcs(self, table_config, dir_prefix, gcs_bucket_prefix, bucket, **kwargs):
        ds = kwargs["ds"]
        ts = kwargs["ts"]
        logging.info(ts)
        sftp_hook = SFTPHook()

        # Create directory if needed and download hourly data
        if not os.path.isdir("sftp_data"):
            os.mkdir("sftp_data")
        my_path = "sftp_data"

        sftp_hook.get_all(dir_prefix, my_path)
        files = glob.glob(my_path + '/**/*.parquet', recursive=True)
        logging.info(files)

        gcs_hook = GCSHook()

        for file in files:
                # Get correct GCS file names
                # drop my_path
                obj = gcs_bucket_prefix + "/" + file.split("/", 1)[1]
                logging.info(obj)
                gcs_hook.upload(
                    bucket=bucket,
                    object=obj,
                    filename=file
                )
        shutil.rmtree(my_path)

    def get_load_script(gcp_project, bq_dataset, bq_table, gcs_bucket, db_name, table_name, date_column, ds):
        full_table_name = f"{bq_dataset}.{bq_table}"
        source_uri_prefix = f"gs://{gcs_bucket}/{db_name}/{table_name}"
        uri_wildcards = f"gs://{gcs_bucket}/{db_name}/{table_name}/{date_column}={ds}/*"
        return f"bq load --source_format=PARQUET --autodetect --hive_partitioning_mode=STRINGS --hive_partitioning_source_uri_prefix={source_uri_prefix} {full_table_name} {uri_wildcards}"

    def validate_extra_options(self):
        pass
        #gcs_source_cfg = from_dict(data_class=GCSSourceConfig, data=self.config.source.extra_options["gcs_source_config"])
        #tables = self.config.tables
        #for table in tables:
        #    gcs_table_cfg = from_dict(data_class=GCSTableConfig, data=table.extra_options.get("gcs_table_config"))