import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime

from gcp_airflow_foundations.base_class.ods_metadata_config import (
    OdsTableMetadataConfig,
)
from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.operators.gcp.ods.ods_merge_table_operator import (
    MergeBigQueryODS,
)

PROJECT_ID = "airflow-framework"
STAGING_DATASET = "af_test_landing_zone"
DATASET = "af_test_ods"
TABLE_NAME = "ga_sessions"
TASK_ID = "test-bq-generic-operator"
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 7, 31))
TEST_DAG_ID = "test-bigquery-operators"
SURROGATE_KEYS = ["visitId", "date", "userId", "clientId"]
SOURCE_TABLE_COLUMNS = [
    "visitorId",
    "visitNumber",
    "visitId",
    "visitStartTime",
    "date",
    "fullVisitorId",
    "userId",
    "clientId",
    "channelGrouping",
    "socialEngagementType",
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


class TestIncrementalUpsertODS(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = EmptyOperator(task_id="dummy", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        self.table_id = f"{TABLE_NAME}_ODS"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryToBigQueryOperator(
            source_project_dataset_tables="airflow-framework.test_tables.ga_sessions_ODS",
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    def test_execute(self):
        ods_upsert_data = MergeBigQueryODS(
            task_id="upsert_ods",
            project_id=PROJECT_ID,
            stg_dataset_name=STAGING_DATASET,
            data_dataset_name=DATASET,
            stg_table_name=TABLE_NAME,
            data_table_name=self.table_id,
            dag_table_id=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column: column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.INCREMENTAL,
            ods_table_config=OdsTableConfig(ods_metadata=OdsTableMetadataConfig()),
            location="US",
        )

        ods_upsert_data.pre_execute(context=self.template_context)

        ods_upsert_data.execute(context=self.template_context)

        original_rows = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])}
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}`
                WHERE date = '20170801'""",
            dialect="standard",
        )

        added_rows = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])}
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}`
                WHERE date = '20170731'""",
            dialect="standard",
        )

        assert len(added_rows) == 1000 and len(original_rows) == 1000


class TestFullUpsertODS(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = EmptyOperator(task_id="dummy", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        self.table_id = f"{TABLE_NAME}_ODS"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryToBigQueryOperator(
            source_project_dataset_tables="airflow-framework.test_tables.ga_sessions_ODS",
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    def test_execute(self):
        ods_upsert_data = MergeBigQueryODS(
            task_id="upsert_ods",
            project_id=PROJECT_ID,
            stg_dataset_name=STAGING_DATASET,
            data_dataset_name=DATASET,
            stg_table_name=TABLE_NAME,
            data_table_name=self.table_id,
            dag_table_id=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column: column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.FULL,
            ods_table_config=OdsTableConfig(ods_metadata=OdsTableMetadataConfig()),
            location="US",
        )

        ods_upsert_data.pre_execute(context=self.template_context)

        ods_upsert_data.execute(context=self.template_context)

        original_rows = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])}
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}`
                WHERE date = '20170801'""",
            dialect="standard",
        )

        added_rows = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT {','.join([column for column in SOURCE_TABLE_COLUMNS])}
                    FROM `{PROJECT_ID}.{DATASET}.{self.table_id}`
                WHERE date = '20170731'""",
            dialect="standard",
        )

        assert len(added_rows) == 1000 and len(original_rows) == 0
