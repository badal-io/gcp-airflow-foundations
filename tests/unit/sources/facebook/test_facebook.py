import os
import pytest
import unittest

import datetime

from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.source_class import (
    sftp_source,
    gcs_source,
    jdbc_dataflow_source,
    oracle_dataflow_source,
    salesforce_source,
    twilio_source,
    facebook
)

from airflow.models.dag import DAG
from airflow.models import (
    DagBag, 
    DagRun, 
    DagTag,
    TaskInstance,
    DagModel
)
from airflow.utils.session import create_session

DEFAULT_DATE = datetime.datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DEV_NULL = '/dev/null'

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        

class TestFacebook(unittest.TestCase):
    def setUp(self):
        clear_db_dags()
        
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = here
        self.config = next(iter(load_tables_config_from_dir(self.conf_location)), None)

        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

    def test_facebook(self):
        facebook_dag_builder = facebook.FacebooktoBQDagBuilder(
            default_task_args=self.args, 
            config=self.config
        )

        for table_config in self.config.tables:
            with DAG(
                TEST_DAG_ID, 
                default_args=self.args, 
                schedule_interval='@once'
            ) as dag:

                op = facebook_dag_builder.get_bq_ingestion_task(
                    dag=dag,
                    table_config=table_config
                )

                assert op is not None