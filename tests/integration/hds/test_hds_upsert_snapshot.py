import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime

from gcp_airflow_foundations.base_class.hds_metadata_config import (
    HdsTableMetadataConfig,
)
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.time_partitioning import TimePartitioning
from gcp_airflow_foundations.operators.gcp.hds.hds_merge_table_operator import (
    MergeBigQueryHDS,
)

PROJECT_ID = "airflow-framework"
STAGING_DATASET = "af_test_landing_zone"
DATASET = "af_test_hds"
TABLE_NAME = "ga_sessions"
TASK_ID = "test-bq-generic-operator"
DEFAULT_DATE = pytz.utc.localize(datetime(2017, 8, 1))
TEST_DAG_ID = "test-bigquery-operators"

SURROGATE_KEYS = ["visitId", "fullVisitorId"]
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


class TestUpsertSnapshotHDS(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = DummyOperator(task_id="dummy", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()

        self.table_id = f"{TABLE_NAME}_HDS_Snapshot"

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

        BigQueryHook().insert_job(
            sql="""SELECT * EXCEPT(af_metadata_expired_at), TIMESTAMP_TRUNC('2017-07-31T00:00:00+00:00', DAY) AS partition_time FROM `airflow-framework.test_tables.ga_sessions_HDS`""",
            use_legacy_sql=False,
            destination_dataset_table=f"{DATASET}.{self.table_id}",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={
                "type": TimePartitioning.DAY.value,
                "field": "partition_time",
            },
        )

    def test_execute(self):
        hds_table_config = HdsTableConfig(
            hds_table_type=HdsTableType.SNAPSHOT,
            hds_metadata=HdsTableMetadataConfig(),
            hds_table_time_partitioning=TimePartitioning.DAY,
        )

        hds_upsert_data = MergeBigQueryHDS(
            task_id="upsert_hds_snapshot",
            project_id=PROJECT_ID,
            stg_dataset_name=STAGING_DATASET,
            data_dataset_name=DATASET,
            stg_table_name=f"{TABLE_NAME}_2017-07-31",
            data_table_name=self.table_id,
            dag_table_id=self.table_id,
            surrogate_keys=SURROGATE_KEYS,
            columns=SOURCE_TABLE_COLUMNS,
            column_mapping={column: column for column in SOURCE_TABLE_COLUMNS},
            ingestion_type=IngestionType.FULL,
            hds_table_config=hds_table_config,
            location="US",
        )

        hds_upsert_data.pre_execute(context=self.template_context)

        hds_upsert_data.execute(context=self.template_context)

        partitions = BigQueryHook().get_pandas_df(
            sql=f"""
                SELECT partition_id
                    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE table_name = '{self.table_id}'""",
            dialect="standard",
        )

        rows = BigQueryHook().get_pandas_df(
            sql=f"""SELECT * FROM `{PROJECT_ID}.{DATASET}.{self.table_id}`""",
            dialect="standard",
        )

        print(partitions)
        assert (
            len(rows) == 2000
            and "20170731" in list(partitions.partition_id)
            and "20170801" in list(partitions.partition_id)
        )
