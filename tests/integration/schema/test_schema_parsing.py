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

from gcp_airflow_foundations.operators.gcp.schema_parsing.schema_parsing_operator import ParseSchema
from gcp_airflow_foundations.parse_dags import DagParser
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.source_class.schema_source_config import AutoSchemaSourceConfig, GCSSchemaSourceConfig, BQLandingZoneSchemaSourceConfig

TASK_ID = 'test-bq-generic-operator'
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 8, 1))
TEST_DAG_ID = 'test-bigquery-operators'

from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils import timezone

expected_xcom = {
    'source_table_columns': [
        'visitorId', 
        'visitNumber', 
        'visitId', 
        'visitStartTime', 
        'date', 
        'fullVisitorId', 
        'userId', 
        'clientId', 
        'channelGrouping', 
        'socialEngagementType', 
        'customDimensions'
    ],
    'af_test_ods.ga_sessions_ODS': [
        {'name': 'visitorId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'visitNumber', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'visitId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'visitStartTime', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'fullVisitorId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'userId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'channelGrouping', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'socialEngagementType', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'customDimensions', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
            {'name': 'index', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
            {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]}, 
        {'name': 'af_metadata_inserted_at', 'type': 'TIMESTAMP'}, 
        {'name': 'af_metadata_updated_at', 'type': 'TIMESTAMP'}, 
        {'name': 'af_metadata_primary_key_hash', 'type': 'STRING'}, 
        {'name': 'af_metadata_row_hash', 'type': 'STRING'}
    ], 
    'af_test_hds.ga_sessions_HDS': [
        {'name': 'visitorId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'visitNumber', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'visitId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'visitStartTime', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'fullVisitorId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'userId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'channelGrouping', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'socialEngagementType', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'customDimensions', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
            {'name': 'index', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
            {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]}, 
        {'name': 'af_metadata_created_at', 'type': 'TIMESTAMP'}, 
        {'name': 'af_metadata_expired_at', 'type': 'TIMESTAMP'}, 
        {'name': 'af_metadata_row_hash', 'type': 'STRING'}
    ]
}

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()

def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()

    
class TestParseSchema(unittest.TestCase):
    def setUp(self):
        here = os.path.abspath(os.path.dirname(__file__))
        self.conf_location = os.path.join(here, "config")

        configs = load_tables_config_from_dir(self.conf_location)

        self.config = next((i for i in configs), None)

        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('TEST_DAG_ID', default_args=args, schedule_interval='@once')

        self.dag.create_dagrun(
            run_id='test', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.SUCCESS
        )

        task = DummyOperator(task_id='dummy', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    def test_execute(self):
        source_config = self.config.source
        table_config = next((i for i in self.config.tables), None)

        table_config.ods_config.table_id = f'{table_config.table_name}_ODS'
        table_config.hds_config.table_id = f'{table_config.table_name}_HDS'

        operator = ParseSchema(
            task_id=TASK_ID, 
            schema_config=BQLandingZoneSchemaSourceConfig,
            column_mapping=None,
            data_source=source_config,
            table_config=table_config
        )

        return_value = operator.execute(context=self.template_context)

        assert return_value == expected_xcom