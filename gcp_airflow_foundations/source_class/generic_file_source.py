import logging
from abc import abstractmethod
from datetime import datetime
import json
from dacite import from_dict

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.dummy import DummyOperator

from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.base_class.file_source_config import FileSourceConfig
from gcp_airflow_foundations.base_class.file_table_config import FileTableConfig


class GenericFileIngestionDagBuilder(DagBuilder):
    """
    Builds DAGs to load files from a generic file system to BigQuery.
    """
    source_type = "FTP"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type     

    def get_bq_ingestion_task(self, dag, table_config):
        taskgroup = TaskGroup(group_id="ftp_taskgroup")

        tasks = []
        tasks.append(self.metadata_file_sensor(table_config, taskgroup))
        tasks.append(self.flag_file_sensor(table_config, taskgroup))
        tasks.append(self.schema_file_sensor(table_config, taskgroup))
        tasks.append(self.get_file_list_task(table_config, taskgroup))
        tasks.append(self.file_sensor(table_config, taskgroup))
        tasks.append(self.file_ingestion_task(table_config, taskgroup))
        tasks.append(self.load_to_landing_task(table_config, taskgroup))

        if self.config.source.extra_options["file_source_config"]["delete_gcs_files"]:
            tasks.append(self.delete_gcs_files(table_config, taskgroup))

        for task in tasks:
            if task is None:
                tasks.remove(task)

        not_none_tasks = list(filter(None.__ne__, tasks))

        for i in range(len(not_none_tasks)-1):
            not_none_tasks[i] >> not_none_tasks[i+1]

        return taskgroup

    def metadata_file_sensor(self, table_config, taskgroup):
        """
        Implements a sensor for either the metadata file specified in the table config, which specifies 
        the parameterized file names to ingest.
        """
        if "metadata_file" in table_config.extra_options.get("file_table_config"):
            metadata_file_name = table_config.extra_options.get("file_table_config")["metadata_file"]
            bucket = self.config.source.extra_options["gcs_bucket"]
            timeout = self.config.source.extra_options["file_source_config"]["sensor_timeout"]

            return GCSObjectExistenceSensor(
                task_id="wait_for_metadata_file",
                bucket=bucket,
                object=metadata_file_name,
                task_group=taskgroup,
                timeout=timeout
            )
        else:
            return None

    @abstractmethod
    def flag_file_sensor(self, table_config):
        """
        Implements an Airflow sensor to wait for optional flag files for ingestion.
        e.g. for .PARQUET file ingestion, waiting for a _SUCCESS file is part of a common flow.
        """
        pass

    def schema_file_sensor(self, table_config, taskgroup):
        """
        Implements an Airflow sensor to wait for an (optional) schema file in GCS
        """
        bucket = self.config.source.extra_options["gcs_bucket"]
        schema_file_name = None
        timeout = self.config.source.extra_options["file_source_config"]["sensor_timeout"]
        if "schema_file" in table_config.extra_options.get("file_table_config"):
            schema_file_name = table_config.extra_options.get("file_table_config")["schema_file"]
            return GCSObjectExistenceSensor(
                task_id="wait_for_schema_file",
                bucket=bucket,
                object=schema_file_name,
                task_group=taskgroup,
                timeout=timeout
            )
        else:
            return None

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

    @abstractmethod
    def delete_gcs_files(table_config, taskgroup):
        pass

    def get_file_list_task(self, table_config, taskgroup):
        return PythonOperator(
            task_id="get_file_list",
            op_kwargs={"table_config": table_config},
            python_callable=self.get_list_of_files,
            task_group=taskgroup
        )

    def get_list_of_files(self, table_config, **kwargs):
        gcs_hook = GCSHook()
        airflow_date_template = self.config.source.extra_options["file_source_config"]["airflow_date_template"]
        if airflow_date_template == "ds":
            ds = kwargs["ds"]
        else:
            ds = kwargs["prev_ds"]
        ds = datetime.strptime(ds, "%Y-%m-%d").strftime(self.config.source.extra_options["file_source_config"]["date_format"])
        # XCom push the list of files
        # overwrite if in table_config
        dir_prefix = table_config.extra_options.get("file_table_config")["directory_prefix"]
        dir_prefix = dir_prefix.replace("{{ ds }}", ds)
        
        if self.config.source.extra_options["file_source_config"]["source_format"] == "PARQUET":
            file_list = [dir_prefix]
            kwargs['ti'].xcom_push(key='file_list', value=file_list)
            return
        else:
            bucket = self.config.source.extra_options["gcs_bucket"]
            if "metadata_file" in table_config.extra_options.get("file_table_config"):
                metadata_file_name = table_config.extra_options.get("file_table_config")["metadata_file"]
                metadata_file = gcs_hook.download(bucket_name=bucket, object_name=metadata_file_name, filename="metadata.csv")
                file_list = []
                with open('metadata.csv', newline='') as f:
                    for line in f:
                        file_list.append(line.strip())
            else:
                templated_file_name = self.config.source.extra_options["file_source_config"]["file_name_template"]
                templated_file_name = templated_file_name.replace("{{ TABLE_NAME }}", table_config.table_name)
                file_list = [templated_file_name]

            # support replacing files with current dates
            file_list[:] = [file.replace("{{ ds }}", ds) if "{{ ds }}" in file else file for file in file_list]
            # add dir prefix to files
            file_list[:] = [dir_prefix + file for file in file_list]
            logging.info(file_list)

        kwargs['ti'].xcom_push(key='file_list', value=file_list)

    def load_to_landing_task(self, table_config, taskgroup):
        return PythonOperator(
            task_id="load_gcs_to_landing_zone",
            op_kwargs={"table_config": table_config},
            python_callable=self.load_to_landing,
            task_group=taskgroup
        )    

    def load_to_landing(self, table_config, **kwargs):
        gcs_hook = GCSHook()

        # Parameters
        ds = kwargs['ds']
        ti = kwargs['ti']

        data_source = self.config.source
        bucket = data_source.extra_options["gcs_bucket"]
        source_format = data_source.extra_options["file_source_config"]["source_format"]
        field_delimeter = data_source.extra_options["file_source_config"]["delimeter"]
        gcp_project = data_source.gcp_project
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset 
        landing_table_name = table_config.landing_zone_table_name_override
        table_name = table_config.table_name
        destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}" + f"_{ds}"
        files_to_load = ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list')
        gcs_bucket_prefix = data_source.extra_options["file_source_config"]["gcs_bucket_prefix"]
        if gcs_bucket_prefix is None:
            gcs_bucket_prefix =  ""
        if not gcs_bucket_prefix == "":
            gcs_bucket_prefix += "/"
        destination_path_prefix = gcs_bucket_prefix + table_name + "/" + ds 
        logging.info(destination_path_prefix)

        if "parquet_upload_option" in table_config.extra_options.get("file_table_config"):
            parquet_upload_option = table_config.extra_options.get("file_table_config")["parquet_upload_option"]
        else:
            parquet_upload_option = "BASH"

        source_format = self.config.source.extra_options["file_source_config"]["source_format"]
        if source_format == "PARQUET" and parquet_upload_option == "BASH":
            date_column = table_config.extra_options.get("sftp_table_config")["date_column"]
            gcs_bucket_prefix = data_source.extra_options["file_source_config"]["gcs_bucket_prefix"]
            # bq load command if parquet
            partition_prefix = ti.xcom_pull(key='partition_prefix', task_ids='ftp_taskgroup.load_sftp_to_gcs')
            command = self.get_load_script(gcp_project, landing_dataset, landing_table_name + f"_{ds}", bucket, gcs_bucket_prefix, partition_prefix, table_name, date_column, ds)
            logging.info(command)
            try:
                bash = BashOperator(
                    task_id="import_files_to_bq_landing",
                    bash_command=command
                )
                bash.execute(context=kwargs)
            except Exception:
                logging.info(f"Load into BQ landing zone failed.")
        else:
            # gcs->bq operator else
            if self.config.source.extra_options["file_source_config"]["file_prefix_filtering"]:
                for i in range(len(files_to_load)):
                    matching_gcs_files = gcs_hook.list(bucket_name=bucket, prefix=files_to_load[i]) 
                    if len(matching_gcs_files) > 1:
                        raise AirflowException(f"There is more than one matching file with the prefix {files_to_load[i]} in the bucket {bucket}")
                    files_to_load[i] = matching_gcs_files[0]

            schema_file_name = None
            if "schema_file" in table_config.extra_options.get("file_table_config"):
                schema_file_name = table_config.extra_options.get("file_table_config")["schema_file"]

            allow_quoted_newlines = False
            if "allow_quoted_newlines" in table_config.extra_options.get("file_table_config"):
                allow_quoted_newlines = table_config.extra_options.get("file_table_config")["allow_quoted_newlines"]

            if parquet_upload_option == "GCS" and source_format == "PARQUET":
                # overwrite files_to_upload
                partition_prefix = ti.xcom_pull(key='partition_prefix', task_ids='ftp_taskgroup.load_sftp_to_gcs')
                files_to_load = gcs_hook.list(bucket_name=bucket, prefix=destination_path_prefix)
                logging.info(files_to_load)

            # Get files to load from metadata file
            if schema_file_name:
                schema_file = gcs_hook.download(bucket_name=bucket, object_name=schema_file_name)
                # Only supports json schema file format - add additional support if required
                schema_fields = json.loads(schema_file)
                gcs_to_bq = GCSToBigQueryOperator(
                    task_id='import_files_to_bq_landing',
                    bucket=bucket,
                    source_objects=files_to_load,
                    source_format=source_format,
                    schema_fields=schema_fields,
                    field_delimiter=field_delimeter,
                    destination_project_dataset_table=destination_table,
                    allow_quoted_newlines=allow_quoted_newlines,
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
                    field_delimiter=field_delimeter,
                    destination_project_dataset_table=destination_table,
                    allow_quoted_newlines=allow_quoted_newlines,
                    write_disposition='WRITE_TRUNCATE',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=1,
                )
            gcs_to_bq.execute(context=kwargs) 

            kwargs['ti'].xcom_push(key='loaded_files', value=files_to_load)


    def get_load_script(self, gcp_project, landing_dataset, landing_table_name, bucket, gcs_bucket_prefix, partition_prefix, table_name, date_column, ds):
        if not partition_prefix == "":
            partition_prefix += "/"
        full_table_name = f"{landing_dataset}.{landing_table_name}"
        source_uri_prefix = f"gs://{bucket}/{gcs_bucket_prefix}{table_name}/{ds}"
        uri_wildcards = f"gs://{bucket}/{gcs_bucket_prefix}{table_name}/{ds}/{partition_prefix}*"
        command = f"bq load --source_format=PARQUET --autodetect --hive_partitioning_mode=STRINGS --hive_partitioning_source_uri_prefix={source_uri_prefix} {full_table_name} {uri_wildcards}"
        logging.info(command)
        return command

    def validate_extra_options(self):
        # try and parse as FTPSourceConfig
        file_source_config = from_dict(data_class=FileSourceConfig, data=self.config.source.extra_options["file_source_config"])

        tables = self.config.tables
        for table_config in tables:
            # try and parse as FTPTableConfig
            file_table_config = from_dict(data_class=FileTableConfig, data=table_config.extra_options.get("file_table_config"))
            