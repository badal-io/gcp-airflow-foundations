import datetime
import logging
import pytest

from gcp_airflow_foundations.parse_dags import DagParser
from tests.unit.conftest import validate_linear_task_order, compare_deps


def test_load_config(sample_dags):
    assert isinstance(sample_dags, dict)
    assert len(sample_dags) == 7


def test_start_date(gcs_dag):
    """Check that start_date_override works for table config"""
    assert (
        gcs_dag.default_args["start_date"].date()
        == datetime.datetime.strptime("2022-01-03", "%Y-%m-%d").date()
    )


def test_gcs_tasks(gcs_dag):
    """Check tasks"""

    validate_linear_task_order(
        gcs_dag,
        [
            "users.ftp_taskgroup.get_file_list",
            "users.ftp_taskgroup.wait_for_files_to_ingest",
            "users.ftp_taskgroup.load_gcs_to_landing_zone",
            "users.schema_parsing",
            "users.create_ods_merge_taskgroup.run_ods_tasks",
            "users.delete_staging_table",
            "users.done",
        ],
    )


def test_gcs_templated_tasks_full(gcs_templated_source_level_dag_full):
    """Check tasks"""

    validate_linear_task_order(
        gcs_templated_source_level_dag_full,
        [
            "test_table_full.ftp_taskgroup.get_file_list",
            "test_table_full.ftp_taskgroup.wait_for_files_to_ingest",
            "test_table_full.ftp_taskgroup.load_gcs_to_landing_zone",
            "test_table_full.schema_parsing",
            "test_table_full.create_ods_merge_taskgroup.run_ods_tasks",
            "test_table_full.delete_staging_table",
            "test_table_full.done",
        ],
    )


def test_gcs_templated_tasks_incremental(gcs_templated_source_level_dag_incremental):
    """Check tasks"""

    validate_linear_task_order(
        gcs_templated_source_level_dag_incremental,
        [
            "test_table_incremental.ftp_taskgroup.get_file_list",
            "test_table_incremental.ftp_taskgroup.wait_for_files_to_ingest",
            "test_table_incremental.ftp_taskgroup.load_gcs_to_landing_zone",
            "test_table_incremental.schema_parsing",
            "test_table_incremental.create_ods_merge_taskgroup.run_ods_tasks",
            "test_table_incremental.delete_staging_table",
            "test_table_incremental.done",
        ],
    )


def test_gcs_tasks_with_dlp(gcs_dlp_dag):
    """Check  ODS DLP tasls"""

    validate_linear_task_order(
        gcs_dlp_dag,
        ordered_task_ids=[
            "users.ftp_taskgroup.get_file_list",
            "users.ftp_taskgroup.wait_for_files_to_ingest",
            "users.ftp_taskgroup.load_gcs_to_landing_zone",
            "users.schema_parsing",
            "users.create_ods_merge_taskgroup.run_ods_tasks",
            "users.delete_staging_table",
            "users.create_dlp_dataset",
            "users.dlp_policy_tags.check_if_should_run_dlp",
            "users.dlp_policy_tags.delete_old_dlp_results_ods",
            "users.dlp_policy_tags.scan_table_ods",
            "users.dlp_policy_tags.read_dlp_results_ods",
            "users.dlp_policy_tags.update_bq_policy_tags_ods",
        ],
        ignore_tasks_ids=["users.done"],
    )

    compare_deps(
        gcs_dlp_dag.get_task("users.done"),
        upstream_deps=[
            "users.dlp_policy_tags.check_if_should_run_dlp",
            "users.dlp_policy_tags.update_bq_policy_tags_ods",
        ],
        downstream_dps=[],
    )


def test_gcs_tasks_with_dlp_and_hds(gcs_dlp_ods_dag):
    """Check ODS + HDS DLP tasks"""
    compare_deps(
        gcs_dlp_ods_dag.get_task("users.dlp_policy_tags.check_if_should_run_dlp"),
        upstream_deps=["users.create_dlp_dataset"],
        downstream_dps=[
            "users.done",
            "users.dlp_policy_tags.delete_old_dlp_results_ods",
            "users.dlp_policy_tags.delete_old_dlp_results_hds",
        ],
    )

    compare_deps(
        gcs_dlp_ods_dag.get_task("users.done"),
        upstream_deps=[
            "users.dlp_policy_tags.check_if_should_run_dlp",
            "users.dlp_policy_tags.update_bq_policy_tags_ods",
            "users.dlp_policy_tags.update_bq_policy_tags_hds",
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
def gcs_templated_source_level_dag_full(sample_dags):
    return sample_dags["dags:source:GCSTemplatedSourceFull.GCSTemplatedSourceFull.TEST"]


@pytest.fixture(scope="session")
def gcs_templated_source_level_dag_incremental(sample_dags):
    return sample_dags["dags:source:GCSTemplatedSourceIncremental.GCSTemplatedSourceIncremental.TEST"]


@pytest.fixture(scope="session")
def gcs_dlp_dag(sample_dags):
    return sample_dags["dags:source:GCSWithDlp.GCSWithDlp.users"]


@pytest.fixture(scope="session")
def gcs_dlp_ods_dag(sample_dags):
    return sample_dags["dags:source:GCSWithHdsAndDlp.GCSWithHdsAndDlp.users"]


@pytest.fixture(scope="session")
def gcs_dag_task_ids(gcs_dag):
    return list(map(lambda task: task.task_id, gcs_dag.tasks))
