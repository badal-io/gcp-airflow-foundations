from genericpath import exists
from keyword import kwlist
import os
import unittest

from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.source_class import gcs_source
from datetime import datetime
from gcp_airflow_foundations.base_class.file_source_config import FileSourceConfig
from gcp_airflow_foundations.base_class.file_table_config import FileTableConfig
from dacite import from_dict
from airflow.models.dag import DAG
from airflow.models import DagRun, DagTag, TaskInstance, DagModel
from airflow.utils.session import create_session
from airflow.utils.task_group import TaskGroup

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
DEV_NULL = "/dev/null"


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestGcs(unittest.TestCase):
    def setUp(self):
        clear_db_dags()

        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = here
        self.config = next(iter(load_tables_config_from_dir(self.conf_location)), None)

        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}

    def test_validate_yaml_good(self):
        # Validate options
        assert self.config.source.source_type == "GCS"
        assert self.config.source.name is not None

        # Validating extra options
        file_source_config = from_dict(data_class=FileSourceConfig, data=self.config.source.extra_options["file_source_config"])
        assert file_source_config.airflow_date_template == "ds"
        assert file_source_config.date_format == "%Y-%m-%d"
        assert file_source_config.delete_gcs_files is not True
        assert file_source_config.gcs_bucket_prefix == ""

        tables = self.config.tables
        for table_config in tables:
            assert table_config.table_name is not None
            file_table_config = from_dict(data_class=FileTableConfig, data=table_config.extra_options.get("file_table_config"))
            assert isinstance(file_table_config.directory_prefix, str)

    def test_gcs_file_sensor_good(self):
        gcs_dag_builder = gcs_source.GCSFileIngestionDagBuilder(
            default_task_args=self.args, config=self.config
        )

        for table_config in self.config.tables:
            with DAG(
                TEST_DAG_ID, default_args=self.args, schedule_interval="@once"
            ) as dag:
                task_group = TaskGroup("test", dag=dag)
                file_sensor = gcs_dag_builder.file_sensor(table_config, task_group)
                assert file_sensor is not None
                assert file_sensor.bucket == "public-gcp-airflow-foundation-samples"
                assert file_sensor.objects == "{{ ti.xcom_pull(key='file_list', task_ids='users.ftp_taskgroup.get_file_list') }}"

    def test_gcs_list_files_good(self):
        gcs_dag_builder = gcs_source.GCSFileIngestionDagBuilder(
            default_task_args=self.args, config=self.config
        )

        for table_config in self.config.tables:
            with DAG(
                TEST_DAG_ID, default_args=self.args, schedule_interval="@once"
            ) as dag:
                task_group = TaskGroup("test", dag=dag)
                file_sensor = gcs_dag_builder.file_sensor(table_config, task_group)
                task_instance = TaskInstance(file_sensor, execution_date=datetime.now())
                task_instance.xcom_push(key='file_list', value='1')
                gcs_dag_builder.get_list_of_files(table_config, ds="2022-04-19", ti=task_instance)
                file_list = task_instance.xcom_pull(key='file_list')
                assert file_list == ["users.csv"]
