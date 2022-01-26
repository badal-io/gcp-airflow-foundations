import logging
import pytz
import unittest
from airflow.models import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from bq_test_utils import insert_to_bq_from_dict
from datetime import datetime
from google.cloud import bigquery
from pytest_testconfig import config
from test_utils import cleanup_xcom, clear_db_dags

from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig, PolicyTagConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.operators.gcp.dlp.dlp_to_datacatalog_taskgroup import dlp_to_datacatalog_builder, dlp_policy_tag_taskgroup_name
from tests.integration.conftest import run_task
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator


TEST_TABLE = "test_table"

DEFAULT_DATE = pytz.utc.localize(datetime(2015, 1, 1))


def policy_tag(location=None, taxonomy=None, tag=None):
    location = location or config['gcp']['location']
    taxonomy = taxonomy or config['dlp']['taxonomy']
    tag = tag or config['dlp']['tag']
    return f'projects/airflow-framework/locations/{location}/taxonomies/{taxonomy}/policyTags/{tag}'


TEST_SCHEMA = [
    {"name": "customerID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    {"name": "city_name", "type": "STRING", "mode": "NULLABLE"}
]
TEST_SCHEMA_WITH_POLICY_TAG = [
    {"name": "customerID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "email", "type": "STRING", "mode": "NULLABLE", "policyTags": {'names': [policy_tag()]}},
    {"name": "city_name", "type": "STRING", "mode": "NULLABLE", "policyTags": {'names': [policy_tag()]}}
]
TEST_SCHEMA_WITH_POLICY_TAG_2 = [
    {"name": "customerID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "email", "type": "STRING", "mode": "NULLABLE",
     "policyTags": {'names': [policy_tag(tag=config['dlp']['wrong_tag'])]}},
    {"name": "city_name", "type": "STRING", "mode": "NULLABLE", "policyTags": {'names': [policy_tag()]}}
]

MOCK_DLP_DATA = [
    {
        "customerID": "customer_1",
        "email": "c1@gmail.com",
        "city_name": "Toronto"
    },
    {
        "customerID": "customer_2",
        "email": "c2@gmail.com",
        "city_name": "Montreal"
    },
    {
        "customerID": "customer_3",
        "email": "c3@hotmail.com",
        "city_name": "Ottawa"
    },
]


# TODO:  Refactor with fixtures to reuse common functionality
class TestDlp(unittest.TestCase):

    # @pytest.fixture(autouse=True)
    def setUp(self):
        super().setUp()
        logging.info("setUp")

        self.dag = DAG('test_dag',
                       default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
                       render_template_as_native_obj=True)

        # self.addCleanup(self.dag.clear)
        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        cleanup_xcom()
        clear_db_dags()
        # setup_test_dag(self)
        self.client = bigquery.Client(project=config["gcp"]["project_id"])
        self.hook = BigQueryHook(gcp_conn_id='google_cloud_default')

        # print_dag_runs()
        # logging.info(f"Context is {self.template_context}")

    def doCleanups(self):
        cleanup_xcom()
        clear_db_dags()

    def create_dlp_dag(self, dag, project_id, dataset_id, target_table_id):
        template_name = config['dlp']['template']

        dlp_taskgroup = TaskGroup(dlp_policy_tag_taskgroup_name(), dag=dag)
        done = DummyOperator(task_id="done", trigger_rule=TriggerRule.ALL_DONE)


        dlp_source_config = DlpSourceConfig(
            results_dataset_id=dataset_id,
            template_name=template_name,
            rows_limit_percent=10,
            policy_tag_config=PolicyTagConfig(
                location=config['gcp']['location'],
                taxonomy=config['dlp']['taxonomy'],
                tag=config['dlp']['tag']
            )
        )

        dlp_table_config = DlpTableConfig().set_source_config(dlp_source_config)

        dlp_tasks = dlp_to_datacatalog_builder(
            taskgroup= dlp_taskgroup,
            datastore="test",
            project_id=project_id,
            table_id=target_table_id,
            dataset_id=dataset_id,
            table_dlp_config=dlp_table_config,
            dag=dag
        )

        delete_old_dlp_results_task = dlp_taskgroup.children["dlp_policy_tags.delete_old_dlp_results_test"]
        scan_table_task = dlp_taskgroup.children["dlp_policy_tags.scan_table_test"]
        read_dlp_results_task = dlp_taskgroup.children["dlp_policy_tags.read_dlp_results_test"]
        update_tags_task = dlp_taskgroup.children["dlp_policy_tags.update_bq_policy_tags_test"]

        return {
            "delete_old_dlp_results_task": delete_old_dlp_results_task,
            "scan_table_task": scan_table_task,
            "read_dlp_results_task": read_dlp_results_task,
            "update_tags_task": update_tags_task
        }

    def test_dlp(self):
        table_id = "test_table"
        project_id = config["gcp"]["project_id"]
        dataset_id = f"{config['bq']['dataset_prefix']}"

        insert_to_bq_from_dict(MOCK_DLP_DATA, project_id, dataset_id, table_id)

        tasks = self.create_dlp_dag(self.dag, project_id, dataset_id, table_id)

        # ti_dlp_results = run_task(tasks['read_dlp_results_task'])

        # xcom_pull_res = ti.xcom_pull(task_ids='dlp_scan_table.read_dlp_results')

        schema = self.get_table_schema(project_id, dataset_id, table_id)
        assert schema == TEST_SCHEMA_WITH_POLICY_TAG

        logging.info(f"TEST_SCHEMA_WITH_POLICY_TAG_2 {TEST_SCHEMA_WITH_POLICY_TAG_2}")

        # change table schema
        self.update_table_schema(project_id, dataset_id, table_id, TEST_SCHEMA_WITH_POLICY_TAG_2)

        schema = self.get_table_schema(project_id, dataset_id, table_id)
        assert schema == TEST_SCHEMA_WITH_POLICY_TAG_2
        assert schema != TEST_SCHEMA_WITH_POLICY_TAG

        # run update tags task again and check that schema has been fixed
        run_task(tasks['update_tags_task'])
        schema = self.get_table_schema(project_id, dataset_id, table_id)
        assert schema == TEST_SCHEMA_WITH_POLICY_TAG

        # TODO: Fix this. Right now clearing tags doesn't work
        # # fake empty DLP result
        # ti_dlp_results.xcom_push(key=XCOM_RETURN_KEY, value=[])
        # run_task(tasks['update_tags_task'])
        #
        # # All policy tags should have been removed
        # schema = self.get_table_schema(project_id, dataset_id, table_id)
        # assert schema == TEST_SCHEMA

        self.doCleanups()

    def update_table_schema(self, project_id, dataset_id, table_id, schema):
        self.hook.update_table_schema(
            dataset_id=dataset_id,
            table_id=table_id,
            schema_fields_updates=schema,
            include_policy_tags=True
        )

    def get_table_schema(self, project_id, dataset_id, table_id):
        return self.client.get_table(f"{project_id}.{dataset_id}.{table_id}").to_api_repr()['schema']['fields']
