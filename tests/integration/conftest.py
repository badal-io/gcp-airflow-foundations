from pytest_testconfig import config
import os
import sys

from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG
from airflow.utils.state import State

import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '../test_utils'))

DEFAULT_DATE = datetime(2015, 1, 1)

def test_dag():
    return DAG(dag_id="testdag", start_date=datetime.now())


# def run_task(task, assert_success=True):
#     ti = TaskInstance(task=task, execution_date=datetime.now())
#     ti.run(ignore_ti_state=True)
#     if assert_success:
#         assert ti.state == State.SUCCESS
#     return ti

def run_task(task, assert_success=True):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
    task.execute(ti.get_template_context())
    # if assert_success:
    #     assert ti.state == State.SUCCESS
    return ti


def _get_field(f, default=None):
    conn_extras: dict = {
        "extra__google_cloud_platform__project": config["gcp"]["project_id"],
        "project": config["gcp"]["project_id"],
    }
    res = conn_extras.get(f)
    return res

def postfix():
    t = datetime.now()
    return t.strftime("%b_%d_%Y_%H_%M_%S")


# @pytest.fixture(scope="session")
# def dag():
#     return test_dag()


# @pytest.fixture(scope="function")
# def mock_connection(mocker):
#     mocker.patch.object(GoogleCloudBaseHook, "_get_field", side_effect=_get_field)
#
#
# @pytest.fixture(scope="session")
# def mock_connection_session(session_mocker):
#     session_mocker.patch.object(
#         GoogleCloudBaseHook, "_get_field", side_effect=_get_field
#     )

# @pytest.fixture(scope="session")
# def test_dataset():
#     project_id = config["gcp"]["project_id"]
#     dataset_name = f"{config['bq']['dataset_prefix']}_{postfix()}"
#     new_dataset = BigQueryCreateEmptyDatasetOperator(
#         project_id=project_id,
#         dataset_id=dataset_name,
#         #dataset_reference={"friendlyName": "Test dataset for Airflow"},
#         task_id="create_test_dataset",
#         dag=test_dag(),
#     )
#     run_task(new_dataset, False)
#
#     yield dataset_name
#     delete_temp_data = BigQueryDeleteDatasetOperator(
#         project_id=config['gcp']['project_id'],
#         dataset_id=config['bq']['dataset_id'],
#         delete_contents=True, # Force the deletion of the dataset as well as its tables (if any).
#         task_id='delete_create_test_dataset',
#         dag=test_dag())
#     run_task(delete_temp_data, False)


# @pytest.fixture(scope="session")
# def project_id():
#     connection = BaseHook.get_connection("google-cloud-default")
#     extra = json.loads(connection.extra)
#     return extra["extra__google_cloud_platform__project"]


def run_task_with_pre_execute(task):
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.pre_execute(ti.get_template_context())
    task.prepare_for_execution().execute(ti.get_template_context())
    return ti
#
# # @pytest.fixture(scope="session")
# # def config():
# #     here = os.path.abspath(os.path.dirname(__file__))
# #
# #     path_parent = os.path.dirname(here)
# #
# #     conf_location = os.path.join(path_parent, "config/gcs_customer_data.yaml")
# #
# #     config = load_tables_config(conf_location)
# #
# #     return config
#
# @pytest.fixture(scope="session")
# def staging_dataset():
#     return "test_stg"
#     #return config.source.landing_zone_options.landing_zone_dataset
#
# @pytest.fixture(scope="session")
# def target_dataset():
#     return "test"
#     #return config.source.dataset_data_name
#
# @pytest.fixture(scope="session")
# def target_table_id():
#     return "test_table"
#
# @pytest.fixture(scope="session")
# def mock_data_rows():
#     rows = [
#         {
#             "customerID": "customer_1",
#             "key_id": 1,
#             "city_name": "Toronto"
#         },
#         {
#             "customerID": "customer_2",
#             "key_id": 2,
#             "city_name": "Montreal"
#         },
#         {
#             "customerID": "customer_3",
#             "key_id": 3,
#             "city_name": "Ottawa"
#         },
#     ]
#
#     return rows
