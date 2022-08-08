import logging
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateDLPJobOperator
from airflow.utils.task_group import TaskGroup
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.table import TableReference

from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.operators.branch.branch_on_cron_operator import (
    BranchOnCronOperator,
)
from gcp_airflow_foundations.operators.gcp.dlp.dlp_helpers import (
    results_to_bq_policy_tags,
)
from gcp_airflow_foundations.operators.gcp.dlp.dlp_job_helpers import (
    build_inspect_job_config,
)
from gcp_airflow_foundations.operators.gcp.dlp.get_dlp_bq_inspection_results_operator import (
    DlpBQInspectionResultsOperator,
)


def dlp_policy_tag_taskgroup_name():
    return "dlp_policy_tags"


def schedule_dlp_to_datacatalog_taskgroup(
    datastore: str,
    project_id: str,
    table_id: str,
    dataset_id: str,
    table_dlp_config: DlpTableConfig,
    next_task: BaseOperator,
    dag,
):
    """
    We want to schedule DLP to run
    1) First time the table is ingested
    2) On schedule there after
    """

    taskgroup = TaskGroup(dlp_policy_tag_taskgroup_name(), dag=dag)

    dlp_task_group = dlp_to_datacatalog_builder(
        taskgroup=taskgroup,
        datastore=datastore,
        project_id=project_id,
        table_id=table_id,
        dataset_id=dataset_id,
        table_dlp_config=table_dlp_config,
        next_task=next_task,
        dag=dag,
    )

    decide = BranchOnCronOperator(
        task_id="check_if_should_run_dlp",
        follow_task_ids_if_true=dlp_task_group,
        follow_task_ids_if_false=next_task.task_id,
        cron_expression=table_dlp_config.source_config.recurrence_schedule,
        use_task_execution_day=True,
        run_on_first_execution=table_dlp_config.source_config.run_on_first_execution,
        dag=dag,
    )

    decide >> dlp_task_group
    return decide


def schedule_dlp_to_datacatalog_taskgroup_multiple_tables(
    table_configs: list, table_dlp_config: DlpTableConfig, next_task: BaseOperator, dag
):
    """
    Check if DLP should run, and run it on multiple tables
    We want to schedule DLP to run
    1) First time the table is ingested
    2) On schedule there after
    """
    logging.info("schedule_dlp_to_datacatalog_taskgroup_multiple_tables")

    taskgroup = TaskGroup(dlp_policy_tag_taskgroup_name(), dag=dag)

    dlp_tasks = []
    for table_config in table_configs:
        dlp_task = dlp_to_datacatalog_builder(
            taskgroup=taskgroup,
            datastore=table_config["datastore"],
            project_id=table_config["project_id"],
            table_id=table_config["table_id"],
            dataset_id=table_config["dataset_id"],
            table_dlp_config=table_dlp_config,
            next_task=next_task,
            dag=dag,
        )
        dlp_tasks.append(dlp_task)

    dlp_task_ids = list(map(lambda t: t.task_id, dlp_tasks))

    logging.info(
        f"Run BranchOnCronOperator for  {dlp_task_ids} next_task {next_task.task_id}"
    )

    decide = BranchOnCronOperator(
        task_id="check_if_should_run_dlp",
        follow_task_ids_if_true=dlp_task_ids,
        follow_task_ids_if_false=next_task.task_id,
        cron_expression=table_dlp_config.source_config.recurrence_schedule,
        use_task_execution_day=True,
        run_on_first_execution=table_dlp_config.source_config.run_on_first_execution,
        task_group=taskgroup,
        dag=dag,
    )

    decide >> dlp_tasks
    return decide


def dlp_to_datacatalog_builder(
    taskgroup: TaskGroup,
    datastore: str,
    project_id: str,
    table_id: str,
    dataset_id: str,
    table_dlp_config: DlpTableConfig,
    next_task: BaseOperator,
    dag,
) -> TaskGroup:
    """
  Method for returning a Task Group for scannign a table with DLP, and creating BigQuery policy tags based on the results
   1) Scan table with DLP and write results to BigQuery
   2) Schedule future DLP
   3) Read results of DLP scan from BigQuery
   4) Update Policy Tags in BQ
   Returns the first task
  """

    assert table_dlp_config.source_config is not None

    # setup tables vars
    dlp_results_dataset_id = table_dlp_config.source_config.results_dataset_id

    table_ref = TableReference(DatasetReference(project_id, dataset_id), table_id)
    dlp_results_table_ref = TableReference(
        DatasetReference(project_id, dlp_results_dataset_id), f"{table_id}_dlp_results"
    )
    dlp_results_table = f"{dlp_results_table_ref.project}.{dlp_results_table_ref.dataset_id}.{dlp_results_table_ref.table_id}"

    # setup DLP scan vars
    dlp_template_name = table_dlp_config.get_template_name()
    rows_limit_percent = table_dlp_config.get_rows_limit_percent()

    inspect_job = build_inspect_job_config(
        dlp_template_name, table_ref, rows_limit_percent, dlp_results_table_ref
    )

    # 1 First delete the results table
    delete_dlp_results = BigQueryDeleteTableOperator(
        task_id=f"delete_old_dlp_results_{datastore}",
        deletion_dataset_table=dlp_results_table,
        ignore_if_missing=True,
        task_group=taskgroup,
        dag=dag,
    )

    # 2 Scan table
    scan_task = CloudDLPCreateDLPJobOperator(
        task_id=f"scan_table_{datastore}",
        project_id=project_id,
        inspect_job=inspect_job,
        wait_until_finished=True,
        task_group=taskgroup,
        dag=dag,
    )
    # 4. Read results
    read_results_task = DlpBQInspectionResultsOperator(
        task_id=f"read_dlp_results_{datastore}",
        project_id=dlp_results_table_ref.project,
        dataset_id=dlp_results_table_ref.dataset_id,
        table_id=dlp_results_table_ref.table_id,
        do_xcom_push=True,
        min_match_count=table_dlp_config.get_min_match_count(),
        task_group=taskgroup,
        dag=dag,
    )

    # 5. Update policy tags
    update_tags_task = PythonOperator(
        task_id=f"update_bq_policy_tags_{datastore}",
        python_callable=update_bq_policy_tags,  # <--- PYTHON LIBRARY THAT COPIES FILES FROM SRC TO DEST
        task_group=taskgroup,
        dag=dag,
        templates_dict={
            "dlp_results": f"{{{{ti.xcom_pull(task_ids='{read_results_task.task_id}')}}}}",
            # "dlp_results": "{{ti.xcom_pull(task_ids='dlp_policy_tags.read_dlp_results_test')}}",
        },
        op_kwargs={
            "project_id": project_id,
            "dataset_id": table_ref.dataset_id,
            "table_id": table_ref.table_id,
            "policy_tag_config": table_dlp_config.source_config.policy_tag_config,
            "task_ids": read_results_task.task_id,
        },
        provide_context=True,
    )

    delete_dlp_results >> scan_task >> read_results_task >> update_tags_task >> next_task

    return delete_dlp_results


def update_bq_policy_tags(
    project_id,
    dataset_id,
    table_id,
    policy_tag_config,
    gcp_conn_id="google_cloud_default",
    delegate_to=None,
    **context,
):
    dlp_results = context["templates_dict"]["dlp_results"]

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)

    schema_fields_updates = results_to_bq_policy_tags(
        project_id, dlp_results, policy_tag_config
    )

    logging.info(
        f"updating policy tags for {project_id}.{dataset_id}.{table_id} to {schema_fields_updates}"
    )
    hook.update_table_schema(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields_updates=schema_fields_updates,
        include_policy_tags=False  # Seem to work fine with False.
        # When set to True it tries to add a policy tags instead of replacing
    )
