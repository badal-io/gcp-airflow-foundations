import unittest
from unittest import mock
from unittest.mock import MagicMock
import os

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

from gcp_airflow_foundations.operators.gcp.hds.hds_merge_table_operator import MergeBigQueryHDS
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.base_class.hds_metadata_config import HdsTableMetadataConfig
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT_ID = 'airflow-framework'
STAGING_DATASET = 'af_test_landing_zone'
DATASET = 'af_test_hds'
TABLE_NAME = 'ga_sessions'
TASK_ID = 'test-bq-generic-operator'
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 7, 31))
TEST_DAG_ID = 'test-bigquery-operators'

from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils import timezone

SURROGATE_KEYS = ['visitId', 'fullVisitorId']
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

    
class TestIncrementalUpsertSCD2HDS(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

        self.dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        task = DummyOperator(task_id='dummy', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        self.table_id = f'{TABLE_NAME}_HDS'

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables='airflow-framework.test_tables.ga_sessions',
            destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{self.table_id}',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED'
        )


    def test_execute(self):
        hds_table_config = HdsTableConfig(
            hds_table_type=HdsTableType.SCD2,
            hds_metadata=HdsTableMetadataConfig(),
            hds_table_time_partitioning=None
        )

        hds_upsert_data = MergeBigQueryHDS(
            task_id=f"upsert_hds_scd2",
            project_id=PROJECT_ID,
            stg_dataset_name=STAGING_DATASET,
            data_dataset_name=DATASET,
            stg_table_name= f"{TABLE_NAME}_2017-07-31_altered",
            data_table_name=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column:column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.INCREMENTAL,
            hds_table_config=hds_table_config,
            location='US'
        )

        hds_upsert_data.pre_execute(context=self.template_context)

        hds_upsert_data.execute(context=self.template_context)

        result_expired = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT fullVisitorId, channelGrouping, socialEngagementType, af_metadata_created_at, af_metadata_expired_at
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}` 
                WHERE fullVisitorId IN ('0013947734290941970', '0015551984140279375','0019723869512624707')
                    AND af_metadata_expired_at IS NOT NULL""",
            dialect='standard'
        )

        result_added = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT fullVisitorId, channelGrouping, socialEngagementType, af_metadata_created_at, af_metadata_expired_at
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}` 
                WHERE fullVisitorId IN ('0013947734290941970', '0015551984140279375','0019723869512624707')
                    AND af_metadata_expired_at IS NULL""",
            dialect='standard'
        )

        assert len(result_expired) == 3 and len(result_added) == 3

class TestFullUpsertSCD2HDS(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

        self.dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        task = DummyOperator(task_id='dummy', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        self.table_id = f'{TABLE_NAME}_HDS'

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().run_copy(
            source_project_dataset_tables='airflow-framework.test_tables.ga_sessions',
            destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{self.table_id}',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED'
        )

    def test_execute(self):
        hds_table_config = HdsTableConfig(
            hds_table_type=HdsTableType.SCD2,
            hds_metadata=HdsTableMetadataConfig(),
            hds_table_time_partitioning=None
        )

        hds_upsert_data = MergeBigQueryHDS(
            task_id=f"upsert_hds_scd2",
            project_id=PROJECT_ID,
            stg_dataset_name=STAGING_DATASET,
            data_dataset_name=DATASET,
            stg_table_name= f"{TABLE_NAME}_2017-07-31_deleted_rows",
            data_table_name=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column:column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.FULL,
            hds_table_config=hds_table_config,
            location='US'
        )

        hds_upsert_data.pre_execute(context=self.template_context)

        hds_upsert_data.execute(context=self.template_context)

        result_expired = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT fullVisitorId, channelGrouping, socialEngagementType, af_metadata_created_at, af_metadata_expired_at
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}` 
                WHERE af_metadata_expired_at IS NOT NULL""",
            dialect='standard'
        )

        result_added = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT fullVisitorId, channelGrouping, socialEngagementType, af_metadata_created_at, af_metadata_expired_at
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}` 
                WHERE af_metadata_expired_at IS NULL""",
            dialect='standard'
        )

        assert len(result_expired) == 500 and len(result_added) == 500