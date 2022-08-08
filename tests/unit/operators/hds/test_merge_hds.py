import pytz
import unittest
from airflow.models import DAG, TaskInstance, XCom, DagRun, DagTag, DagModel
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

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

TASK_ID = "test-bq-generic-operator"
TEST_DATASET = "test-dataset"
TEST_GCP_PROJECT_ID = "test-project"
TEST_TABLE_ID = "test-table-id"
TEST_STG_TABLE_ID = "test-staging-table-id"
DEFAULT_DATE = pytz.utc.localize(datetime(2021, 1, 1))
TEST_DAG_ID = "test-bigquery-operators"
SCHEMA_FIELDS = [{"name": "column", "type": "STRING"}]


@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


class TestMergeBigQuerySnapshotHDS(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = EmptyOperator(task_id="schema_parsing", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()
        self.ti.xcom_push(key=XCOM_RETURN_KEY, value={TEST_TABLE_ID: SCHEMA_FIELDS})

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        hds_table_config = HdsTableConfig(
            hds_table_type=HdsTableType.SNAPSHOT,
            hds_metadata=HdsTableMetadataConfig(),
            hds_table_time_partitioning=TimePartitioning.DAY,
        )

        operator = MergeBigQueryHDS(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT_ID,
            stg_table_name=TEST_STG_TABLE_ID,
            data_table_name=TEST_TABLE_ID,
            dag_table_id=TEST_TABLE_ID,
            stg_dataset_name=TEST_DATASET,
            data_dataset_name=TEST_DATASET,
            columns=["column"],
            surrogate_keys=["column"],
            column_mapping={"column": "column"},
            column_casting=None,
            new_column_udfs=None,
            ingestion_type=IngestionType.FULL,
            hds_table_config=hds_table_config,
        )

        operator.pre_execute(context=self.template_context)

        operator.execute(MagicMock())

        ds = self.template_context["ds"]

        sql = f"""
            SELECT
                `column` AS `column`,
                CURRENT_TIMESTAMP() AS af_metadata_created_at,
                TIMESTAMP_TRUNC('2021-01-01T00:00:00+00:00', DAY) AS partition_time,
                TO_BASE64(MD5(TO_JSON_STRING(S))) AS af_metadata_row_hash
            FROM `{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_STG_TABLE_ID}` S
        """

        mock_hook.return_value.run_query.assert_called_once_with(
            sql=sql,
            destination_dataset_table=f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}${ds.replace('-', '')}",
            write_disposition="WRITE_TRUNCATE",
            allow_large_results=False,
            flatten_results=None,
            udf_config=None,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=None,
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=None,
        )


class TestMergeBigQuerySCD2HDS(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("TEST_DAG_ID", default_args=args, schedule_interval="@once")

        self.dag.create_dagrun(
            run_id="test",
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
        )

        task = EmptyOperator(task_id="schema_parsing", dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        self.template_context = self.ti.get_template_context()
        self.ti.xcom_push(key=XCOM_RETURN_KEY, value={TEST_TABLE_ID: SCHEMA_FIELDS})

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        hds_table_config = HdsTableConfig(
            hds_table_type=HdsTableType.SCD2,
            hds_metadata=HdsTableMetadataConfig(),
            hds_table_time_partitioning=None,
        )

        operator = MergeBigQueryHDS(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT_ID,
            stg_table_name=TEST_STG_TABLE_ID,
            data_table_name=TEST_TABLE_ID,
            dag_table_id=TEST_TABLE_ID,
            stg_dataset_name=TEST_DATASET,
            data_dataset_name=TEST_DATASET,
            columns=["column_a", "column_b"],
            surrogate_keys=["column_a"],
            column_mapping={"column_a": "column_a", "column_b": "column_b"},
            column_casting=None,
            ingestion_type=IngestionType.FULL,
            hds_table_config=hds_table_config,
        )

        operator.pre_execute(context=self.template_context)

        operator.execute(MagicMock())

        sql = f"""
            MERGE `{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}` T
            USING (SELECT  column_a AS join_key_column_a, column_a,column_b
            FROM `{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_STG_TABLE_ID}`
            UNION ALL
            SELECT
                NULL,
                source.`column_a`,source.`column_b`
                FROM `{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_STG_TABLE_ID}` source
                JOIN `{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}` target
                ON target.column_a=source.column_a
                WHERE (
                        (MD5(TO_JSON_STRING(target.`column_b`)) != MD5(TO_JSON_STRING(source.`column_b`)))
                        AND target.af_metadata_expired_at IS NULL
                    )) S
            ON T.`column_a`=S.`join_key_column_a`
            WHEN MATCHED AND (MD5(TO_JSON_STRING(T.`column_b`)) != MD5(TO_JSON_STRING(S.`column_b`))) THEN UPDATE SET af_metadata_expired_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED BY TARGET THEN INSERT (`column_a`,`column_b`, af_metadata_created_at, af_metadata_expired_at, af_metadata_row_hash)
            VALUES (`column_a`,`column_b`, CURRENT_TIMESTAMP(), NULL, TO_BASE64(MD5(TO_JSON_STRING(S))))
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET T.af_metadata_expired_at = CURRENT_TIMESTAMP()
        """

        mock_hook.return_value.run_query.assert_called_once_with(
            sql=sql,
            destination_dataset_table=None,
            write_disposition="WRITE_APPEND",
            allow_large_results=False,
            flatten_results=None,
            udf_config=None,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_NEVER",
            schema_update_options=None,
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=None,
        )
