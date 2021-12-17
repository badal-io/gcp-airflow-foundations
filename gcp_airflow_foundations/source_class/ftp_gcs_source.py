from dataclasses import fields
from os import X_OK
from urllib.parse import urlparse
from dacite import from_dict
from dataclasses import dataclass

from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from gcp_airflow_foundations.operators.api.sensors.gcs_sensor import GCSObjectListExistenceSensor
from gcp_airflow_foundations.operators.api.sensors.gcs_prefix_sensor import GCSObjectPrefixListExistenceSensor
from gcp_airflow_foundations.source_class.ftp_source import GenericFileIngestionDagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder


class GCSFileIngestionDagBuilder(GenericFileIngestionDagBuilder):
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
    source_type = "GCS"

    def flag_file_sensor(self, table_config, taskgroup):
        if "flag_file_path" in table_config.extra_options.get("ftp_table_config"):
            flag_file_path = table_config.extra_options.get("ftp_table_config")["flag_file_path"]
            bucket = self.config.source.extra_options["gcs_bucket"]
            return GCSObjectExistenceSensor(
                task_id="wait_for_flag_file",
                bucket=bucket,
                object=flag_file_path,
                task_group=taskgroup
            )
        else:
            return DummyOperator(
                task_id="dummy_flag_file_sensor",
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

    def file_sensor(self, table_config, taskgroup):
        """
        Returns an Airflow sensor that waits for the list of files specified by the metadata file provided.
        """
        bucket = self.config.source.extra_options["gcs_bucket"]
        files_to_wait_for = "{{ ti.xcom_pull(key='file_list', task_ids='ftp_taskgroup.get_file_list') }}"

        if self.config.source.extra_options["ftp_source_config"]["file_prefix_filtering"]:
            return GCSObjectPrefixListExistenceSensor(
                task_id="wait_for_files_to_ingest",
                bucket=bucket,
                prefixes=files_to_wait_for,
                task_group=taskgroup
            )
        else:
            return GCSObjectListExistenceSensor(
                task_id="wait_for_files_to_ingest",
                bucket=bucket,
                objects=files_to_wait_for,
                task_group=taskgroup
            )
            