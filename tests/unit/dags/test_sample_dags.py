import datetime
import logging
import pytest

from gcp_airflow_foundations.parse_dags import DagParser
from tests.unit.conftest import validate_linear_task_order, compare_deps


def test_load_config(sample_dags):
    assert isinstance(sample_dags, dict)
    assert len(sample_dags) == 3


def test_start_date(gcs_dag):
    """Check that start_date_override works for table config"""
    assert (
        gcs_dag.default_args["start_date"].date()
        == datetime.datetime.strptime("2022-01-03", "%Y-%m-%d").date()
    )


def test_gcs_tasks(gcs_dag, gcs_dag_task_ids):
    """Check tasks"""

    validate_linear_task_order(
        gcs_dag,
        [
            "ftp_taskgroup.get_file_list",
            "ftp_taskgroup.wait_for_files_to_ingest",
            "ftp_taskgroup.load_gcs_to_landing_zone",
            # 'ftp_taskgroup.delete_gcs_files',
            "schema_parsing",
            "create_ods_merge_taskgroup.create_ods_dataset",
            "create_ods_merge_taskgroup.create_ods_table",
            "create_ods_merge_taskgroup.schema_migration",
            "create_ods_merge_taskgroup.upsert_users",
            "delete_staging_table",
            "done",
        ],
    )


def test_gcs_tasks_with_dlp(gcs_dlp_dag):
    """Check  ODS DLP tasls"""

    validate_linear_task_order(
        gcs_dlp_dag,
        ordered_task_ids=[
            "ftp_taskgroup.get_file_list",
            "ftp_taskgroup.wait_for_files_to_ingest",
            "ftp_taskgroup.load_gcs_to_landing_zone",
            # 'ftp_taskgroup.delete_gcs_files',
            "schema_parsing",
            "create_ods_merge_taskgroup.create_ods_dataset",
            "create_ods_merge_taskgroup.create_ods_table",
            "create_ods_merge_taskgroup.schema_migration",
            "create_ods_merge_taskgroup.upsert_users",
            "delete_staging_table",
            "create_dlp_dataset",
            "dlp_policy_tags.check_if_should_run_dlp",
            "dlp_policy_tags.delete_old_dlp_results_ods",
            "dlp_policy_tags.scan_table_ods",
            "dlp_policy_tags.read_dlp_results_ods",
            "dlp_policy_tags.update_bq_policy_tags_ods",
        ],
        ignore_tasks_ids=["done"],
    )

    compare_deps(
        gcs_dlp_dag.get_task("done"),
        upstream_deps=[
            "dlp_policy_tags.check_if_should_run_dlp",
            "dlp_policy_tags.update_bq_policy_tags_ods",
        ],
        downstream_dps=[],
    )


def test_gcs_tasks_with_dlp_and_hds(gcs_dlp_ods_dag):
    """Check ODS + HDS DLP tasks"""
    compare_deps(
        gcs_dlp_ods_dag.get_task("dlp_policy_tags.check_if_should_run_dlp"),
        upstream_deps=["create_dlp_dataset"],
        downstream_dps=[
            "done",
            "dlp_policy_tags.delete_old_dlp_results_ods",
            "dlp_policy_tags.delete_old_dlp_results_hds",
        ],
    )

    compare_deps(
        gcs_dlp_ods_dag.get_task("done"),
        upstream_deps=[
            "dlp_policy_tags.check_if_should_run_dlp",
            "dlp_policy_tags.update_bq_policy_tags_ods",
            "dlp_policy_tags.update_bq_policy_tags_hds",
        ],
        downstream_dps=[],
    )


@pytest.fixture(scope="session")
def sample_dags():
    parser = DagParser()
    dag_bag = parser.parse_dags()
    logging.info(f"TestSampleDags ${dag_bag}")
    return dag_bag


@pytest.fixture(scope="session")
def gcs_dag(sample_dags):
    return sample_dags["dags:source:SampleGCS.SampleGCS.users"]


@pytest.fixture(scope="session")
def gcs_dlp_dag(sample_dags):
    return sample_dags["dags:source:GCSWithDlp.GCSWithDlp.users"]


@pytest.fixture(scope="session")
def gcs_dlp_ods_dag(sample_dags):
    return sample_dags["dags:source:GCSWithHdsAndDlp.GCSWithHdsAndDlp.users"]


@pytest.fixture(scope="session")
def gcs_dag_task_ids(gcs_dag):
    return list(map(lambda task: task.task_id, gcs_dag.tasks))
