from dataclasses import fields
from os import X_OK
from urllib.parse import urlparse
import logging
from dacite import from_dict
from dataclasses import dataclass
from datetime import datetime
import shutil
import os
import glob

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.python import PythonSensor

from gcp_airflow_foundations.operators.api.hooks.sftp_hook import SFTPHook
from gcp_airflow_foundations.operators.api.sensors.sftp_sensor import SFTPFilesExistenceSensor
from gcp_airflow_foundations.source_class.ftp_source import GenericFileIngestionDagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder
from gcp_airflow_foundations.base_class.sftp_table_config import SFTPTableConfig


class SFTPFileIngestionDagBuilder(GenericFileIngestionDagBuilder):
    """
    Builds DAGs to load files from an SFTP server to a BigQuery Table. Authenticates with the Airflow SFTP provider connection.

    For specifying files to be uploaded, there are 3 options:
        1) A metadata file is provided with templated daily file names (templating supports {{ ds }} and {{ TABLE_NAME }})
        2) The field templated_file_name is provided in the table config (the same templating applies - but only 1 file can be provided in this manner)
        3) A directory can be specified for .PARQUET files only in the table config

    If a metadata file is provided, it can be a fixed file, or can be a new file supplied daily.
    Airflow context variables are supported for the file naming, e.g.
        TABLE_METADATA_FILE_{{ ds }}.csv
    for a metadata file supplied daily.

    The format of the metadata file should be a csv with one column as follows:
        FILE_NAME_1
        ...
        FILE_NAME_N
    with all files to ingest

    If a directory for .PARQUET ingestion is provided, the field directory_prefix should point to the directory where
    the outward-most external partition sits, and the field flag_file_path should point to the path of the _SUCCESS file on the sftp server.
    A mod_time check occurs on the flag file to screen for daily ingestion on the sensor.
    """
    source_type = "SFTP"

    def flag_file_sensor(self, table_config, taskgroup):
        if "flag_file_path" in table_config.extra_options.get("ftp_table_config"):
            dir_prefix = table_config.extra_options.get("ftp_table_config")["directory_prefix"]
            flag_file_path = table_config.extra_options.get("ftp_table_config")["flag_file_path"]
            return self.get_sftp_sensor(table_config, taskgroup, dir_prefix, flag_file_path)
        else:
            return DummyOperator(
                task_id="dummy_flag_file_sensor",
                task_group=taskgroup
            )

    def file_ingestion_task(self, table_config, taskgroup):
        """
        Ingest from SFTP
        """
        dir_prefix = table_config.extra_options.get("ftp_table_config")["directory_prefix"]
        if "flag_file_path" in table_config.extra_options.get("ftp_table_config"):
            flag_file_path = table_config.extra_options.get("ftp_table_config")["flag_file_path"]
        else: 
            flag_file_path = ""
        gcs_bucket_prefix = self.config.source.extra_options["ftp_source_config"]["gcs_bucket_prefix"]
        bucket = self.config.source.extra_options["gcs_bucket"]
        return self.get_sftp_ingestion_operator(table_config, taskgroup, dir_prefix, gcs_bucket_prefix, flag_file_path, bucket)

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
        # If we have a flag file for ingestion, there is no need to check the files specifically
        if "flag_file_path" in table_config.extra_options.get("ftp_table_config"):
            return DummyOperator(
                task_id="dummy_sftp_file_existence_sensor",
                task_group=taskgroup
            )

        bucket = self.config.source.extra_options["gcs_bucket"]
        files_to_wait_for = "{{ ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list') }}"

        return SFTPFilesExistenceSensor(
            task_id="wait_for_files_to_ingest",
            bucket=bucket,
            objects=files_to_wait_for,
            task_group=taskgroup
        )

    def get_sftp_sensor(self, table_config, taskgroup, dir_prefix, flag_file_path):
        return PythonSensor(
            task_id="sftp_sensor",
            op_kwargs={"table_config": table_config,
                       "dir_prefix": dir_prefix,
                       "flag_file_path": flag_file_path},
            python_callable=self.sftp_flag_sensor,
            task_group=taskgroup
        )

    def sftp_flag_sensor(self, table_config, dir_prefix, flag_file_path, **kwargs):
        # Check if the flag file is dated with the execution date or later - if yes, then the files are ready for ingestion.
        # allows backfilling
        ds = kwargs["ds"]
        sftp_hook = SFTPHook()
        dir_prefix = dir_prefix.replace("{{ ds }}", ds)
        mod_time = sftp_hook.get_mod_time(flag_file_path)
        mod_time = datetime.strptime(mod_time, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
        return ds <= mod_time
    
    def get_sftp_ingestion_operator(self, table_config, taskgroup, dir_prefix, gcs_bucket_prefix, flag_file_path, bucket):
        if not flag_file_path == "":
            return PythonOperator(
                task_id="load_sftp_to_gcs_flag",
                op_kwargs={"table_config": table_config,
                        "dir_prefix": dir_prefix,
                        "gcs_bucket_prefix": gcs_bucket_prefix,
                        "bucket": bucket,
                        "flag_file_path": flag_file_path},
                python_callable=self.load_sftp_to_gcs_flag,
                task_group=taskgroup
            )
        else:
            return PythonOperator(
                task_id="load_sftp_to_gcs",
                op_kwargs={"table_config": table_config,
                        "dir_prefix": dir_prefix,
                        "gcs_bucket_prefix": gcs_bucket_prefix,
                        "bucket": bucket},
                python_callable=self.load_sftp_to_gcs,
                task_group=taskgroup
            )
    
    def load_sftp_to_gcs(self, table_config, dir_prefix, gcs_bucket_prefix, bucket, **kwargs):
        ds = kwargs["ds"]
        ti = kwargs["ti"]

        if not os.path.isdir("sftp_data"):
            os.mkdir("sftp_data")
        my_path = "sftp_data"

        table_name = table_config.table_name
        source_path_prefix = dir_prefix + "/*"
        destination_path_prefix = gcs_bucket_prefix + "/" + table_name + "/" + ds 

        sftp_hook = SFTPHook()
        files_to_load = ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list')

        for file in files_to_load:
            sftp_hook.retrieve_file(dir_prefix + "/" + file, my_path + "/" + file)

        gcs_hook = GCSHook()
        local_files = [f for f in os.listdir(my_path) if os.isfile(os.join(my_path, f))]
        for file in files_to_load:
            obj = destination_path_prefix + "/" + file
            gcs_hook.upload(
                bucket_name=bucket,
                object_name=obj,
                filename=my_path + "/" + file
                )
        # delete data
        shutil.rmtree(my_path)

    def load_sftp_to_gcs_flag(self, table_config, dir_prefix, gcs_bucket_prefix, bucket, flag_file_path, **kwargs):
        ds = kwargs["ds"]
        ti = kwargs["ti"]

        airflow_date_template = self.config.source.extra_options["ftp_source_config"]["airflow_date_template"]
        
        # flag to check whether we are loading dates named with {{ prev_ds }} or {{ ds }}
        # landing zone table creation will still use {{ ds }}
        source_file_date = ds
        if airflow_date_template == "prev_ds":
            source_file_date = kwargs["prev_ds"]

        date_column = table_config.extra_options.get("sftp_table_config")["date_column"]
        table_name = table_config.table_name
        source_path_prefix = dir_prefix + "/*"
        destination_path_prefix = gcs_bucket_prefix + "/" + table_name + "/" + ds 
        sftp_hook = SFTPHook()

        # Create directory and download data
        if not os.path.isdir("sftp_data"):
            os.mkdir("sftp_data")
        my_path = "sftp_data"
        os.chmod(my_path, 0o777)

        # get recursive sftp structure w/ full paths
        map = sftp_hook.get_tree_map(dir_prefix)
        if isinstance(map[0], list):
            map = map[0]

        # get list of files, create local directory structure, download
        files_to_upload = [x for x in map if f"{date_column}={source_file_date}" in x]
        logging.info(files_to_upload)
        for remote_file in files_to_upload:
            conn = sftp_hook.get_conn()
            path_to_create = my_path + "/" + remote_file
            path_to_create = path_to_create.rsplit('/', 1)[0]
            if not os.path.exists(path_to_create):
                folders = path_to_create.split("/")
                for i in range(len(folders)):
                    folder_to_create = "/".join(folders[0:i+1])
                    if not os.path.exists(folder_to_create):
                        os.mkdir(folder_to_create)
                        os.chmod(folder_to_create, 0o777)
            sftp_hook.retrieve_file(remote_file, my_path + "/" + remote_file)

        # get list of local files
        files = glob.glob(my_path + '/**/*.parquet', recursive=True)
        logging.info(files)

        # delete prefix up until the external partitions
        local_file_prefix_to_drop = ""
        if not files_to_upload == []:
            local_file_prefix_to_drop = my_path + "/" + dir_prefix + "/"

        flag_file = flag_file_path.split("/")[-1]
        gcs_hook = GCSHook()
        for file in files:
            if not file == flag_file:
                # Get correct GCS file names to upload
                obj = destination_path_prefix + "/" + file.replace(local_file_prefix_to_drop, "")
                logging.info(obj)
                gcs_hook.upload(
                    bucket_name=bucket,
                    object_name=obj,
                    filename=file
                    )
        # delete data
        shutil.rmtree(my_path)
    
        # xcom push the prefix of external partitions, e.g. "par1=val1/par2=val/par3=val3"
        partition_prefix = files[0].replace(local_file_prefix_to_drop, "").rsplit('/', 1)[0]
        kwargs['ti'].xcom_push(key='partition_prefix', value=partition_prefix)

    def validate_extra_options(self):
        super().validate_extra_options()
        tables = self.config.tables
        for table_config in tables:
            # try and parse as SFTPTableConfig
            sftp_table_config = from_dict(data_class=SFTPTableConfig, data=table_config.extra_options.get("sftp_table_config"))