import unittest
from unittest import mock
from unittest.mock import MagicMock
import os

#import pandas

import pytest
from google.cloud.exceptions import Conflict

from datetime import datetime
import pytz

from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
from airflow.models import (
    DAG,
    TaskInstance,
    XCom,
    DagBag, 
    DagRun, 
    DagTag,
    DagModel
)
from airflow.models.xcom import XCOM_RETURN_KEY

from gcp_airflow_foundations.operators.gcp.ods.ods_merge_table_operator import MergeBigQueryODS
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig
from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.parse_dags import DagParser
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

TASK_ID = 'test-bq-generic-operator'
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 8, 1))
TEST_DAG_ID = 'test-bigquery-operators'

from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils import timezone

SURROGATE_KEYS = ['visitId','date','userId','clientId']
SOURCE_TABLE_COLUMNS = [
    'visitorId', 
    'visitNumber', 
    'visitId', 
    'visitStartTime', 
    'date', 
    'fullVisitorId', 
    'userId', 
    'clientId', 
    'channelGrouping', 
    'socialEngagementType'
]

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()

    
class TestUpsertODS(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

        self.dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        task = DummyOperator(task_id='dummy', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

        self.source_config = self.config.source
        self.table_config = next((i for i in self.config.tables), None)

        self.table_id = f'{self.table_config.table_name}_ODS'

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables='airflow-framework.test_tables.ga_sessions_ODS',
            destination_project_dataset_table=f'{self.source_config.gcp_project}.{self.source_config.dataset_data_name}.{self.table_id}',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED'
        )

    def test_execute(self):
        ods_upsert_data = MergeBigQueryODS(
            task_id=f"upsert_ods",
            project_id=self.source_config.gcp_project,
            stg_dataset_name=self.source_config.landing_zone_options.landing_zone_dataset,
            data_dataset_name=self.source_config.dataset_data_name,
            stg_table_name=self.table_config.table_name,
            data_table_name=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column:column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.INCREMENTAL,
            ods_table_config=OdsTableConfig(ods_metadata=OdsTableMetadataConfig()),
            location='US'
        )

        ods_upsert_data.pre_execute(context={'ds':'2017-07-31'})

        ods_upsert_data.execute(context=self.template_context)

        result = BigQueryHook().get_pandas_df(
            sql=f"""SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])} FROM `{self.source_config.gcp_project}.{self.source_config.dataset_data_name}.{self.table_id}`""",
            dialect='standard'
        )

        expected = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])} FROM `airflow-framework.af_test_landing_zone.ga_sessions_2017-07-31`
                    UNION ALL
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])} FROM `airflow-framework.af_test_landing_zone.ga_sessions_2017-08-01`
            """,
            dialect='standard'
        )

        assert result.equals(expected)