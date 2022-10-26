import os
import pytest
import unittest

from gcp_airflow_foundations.operators.gcp.hds.load_hds_taskgroup import hds_builder
from gcp_airflow_foundations.operators.gcp.ods.load_ods_taskgroup import ods_builder
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir

from airflow.utils.session import create_session
from airflow.utils.timezone import datetime

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG, DagTag, DagModel

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestTaskGroupBuilder(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        conf_location = os.path.join(here, "config", "valid")
        self.configs = load_tables_config_from_dir(conf_location)

        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}

    def test_ods_task_group(self):
        for config in self.configs:
            data_source = config.source

            for table in config.tables:
                with DAG(
                    f"{TEST_DAG_ID}.{table.table_name}",
                    default_args=self.args,
                    schedule_interval="@once",
                ) as dag:
                    ods_task_group = ods_builder(
                        project_id=data_source.gcp_project,
                        table_id=table.table_name,
                        dag_table_id=table.table_name,
                        dataset_id=data_source.dataset_data_name,
                        landing_zone_dataset=data_source.landing_zone_options.landing_zone_dataset,
                        landing_zone_table_name_override=None,
                        surrogate_keys=table.surrogate_keys,
                        column_mapping=table.column_mapping,
                        column_casting=table.column_casting,
                        new_column_udfs=table.new_column_udfs,
                        ingestion_type=table.ingestion_type,
                        ods_table_config=table.ods_config,
                        partition_expiration=None,
                        location=data_source.location,
                        dag=dag,
                    )

                assert ods_task_group is not None, "Clould not load the ODS task group"

    def test_hds_task_group(self):
        for config in self.configs:
            data_source = config.source

            for table in config.tables:
                with DAG(
                    f"{TEST_DAG_ID}.{table.table_name}",
                    default_args=self.args,
                    schedule_interval="@once",
                ) as dag:
                    hds_task_group = hds_builder(
                        project_id=data_source.gcp_project,
                        table_id=table.table_name,
                        dag_table_id=table.table_name,
                        dataset_id=data_source.dataset_data_name,
                        landing_zone_dataset=data_source.landing_zone_options.landing_zone_dataset,
                        landing_zone_table_name_override=None,
                        surrogate_keys=table.surrogate_keys,
                        column_mapping=table.column_mapping,
                        column_casting=table.column_casting,
                        new_column_udfs=table.new_column_udfs,
                        ingestion_type=table.ingestion_type,
                        hds_table_config=table.hds_config,
                        partition_expiration=None,
                        location=data_source.location,
                        dag=dag,
                    )

                assert hds_task_group is not None, "Could not load the ODS task group"
