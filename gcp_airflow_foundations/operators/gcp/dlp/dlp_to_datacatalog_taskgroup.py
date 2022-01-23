import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateDLPJobOperator
from airflow.utils.task_group import TaskGroup
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.table import TableReference

from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.operators.gcp.dlp.dlp_helpers import results_to_bq_policy_tags
from gcp_airflow_foundations.operators.gcp.dlp.dlp_job_helpers import build_job_trigger, build_inspect_job_config
from gcp_airflow_foundations.operators.gcp.dlp.get_dlp_bq_inspection_results_operator import \
    DlpBQInspectionResultsOperator
from gcp_airflow_foundations.operators.branch.BranchOnCronOperator import BranchOnCronOperator


def dlp_policy_tag_taskgroup_name():
    return f"dlp_policy_tags_for"

def schedule_dlp_to_datacatalog_taskgroup(
        project_id: str,
        table_id: str,
        dataset_id: str,
        source_dlp_config: DlpSourceConfig,
        table_dlp_config: DlpTableConfig,
        next_task_id: str,
        dag):
    """
    We want to schedule DLP to run
    1) First time the table is ingested
    2) On schedule there after
    """
    dlp_task_group = dlp_to_datacatalog_builder(
        project_id=project_id,
        table_id=table_id,
        dataset_id=dataset_id,
        source_dlp_config=source_dlp_config,
        table_dlp_config = table_dlp_config
    )

    return BranchOnCronOperator(
        task_id="check_if_should_run_dlp",
        follow_task_ids_if_true=dlp_task_group.task_id,
        follow_task_ids_if_false=next_task_id,
        cron_expression=source_dlp_config.recurrence_schedule,
        use_task_execution_day=True,
        run_on_first_execution=source_dlp_config.run_on_first_execution,
        dag=dag,
    )



def dlp_to_datacatalog_builder(
        project_id: str,
        table_id: str,
        dataset_id: str,
        source_dlp_config: DlpSourceConfig,
        table_dlp_config: DlpTableConfig,
        dag) -> TaskGroup:
    """
  Method for returning a Task Group for scannign a table with DLP, and creating BigQuery policy tags based on the results
   1) Scan table with DLP and write results to BigQuery
   2) Schedule future DLP
   3) Read results of DLP scan from BigQuery
   4) Update Policy Tags in BQ
  """

    taskgroup = TaskGroup(dlp_policy_tag_taskgroup_name(), dag= dag)

    # setup tables vars
    dlp_results_dataset_id = source_dlp_config.results_dataset_id

    table_ref = TableReference(DatasetReference(project_id, dataset_id), table_id)
    dlp_results_table_ref = TableReference(DatasetReference(project_id, dlp_results_dataset_id),
                                           f"{table_id}_dlp_results")
    dlp_results_table = f"{dlp_results_table_ref.project}.{dlp_results_table_ref.dataset_id}.{dlp_results_table_ref.table_id}"

    # setup DLP scan vars
    dlp_template_name = table_dlp_config.get_template_name(source_dlp_config)
    scan_job_name = f"af_inspect_{dataset_id}.{table_id}_with_{dlp_template_name}"
    rows_limit_percent = table_dlp_config.get_rows_limit_percent(source_dlp_config)

    inspect_job = build_inspect_job_config(dlp_template_name, table_ref, rows_limit_percent, dlp_results_table_ref)
    job_trigger = build_job_trigger(scan_job_name, dlp_template_name, table_ref, rows_limit_percent,
                                    dlp_results_table_ref, table_dlp_config.get_recurrence(source_dlp_config))

    # 1 First delete the results table
    delete_dlp_results = BigQueryDeleteTableOperator(
        task_id="delete_old_dlp_results",
        deletion_dataset_table=dlp_results_table,
        ignore_if_missing=True,
        task_group=taskgroup,
        dag=dag
    )

    # 2 Scan table
    scan = CloudDLPCreateDLPJobOperator(
        task_id="scan_table",
        project_id=project_id,
        inspect_job=inspect_job,
        wait_until_finished=True,
        task_group=taskgroup,
        dag=dag
    )
    # #3 Schedule periodic scans
    # schedule_scan = CloudDLPCreateJobTriggerOperator(
    #     task_id="create_scan_table_trigger",
    #     project_id=project_id,
    #     job_trigger=job_trigger,
    #     task_group=taskgroup,
    #     dag=dag
    # )

    # 4. Read results
    read_results = DlpBQInspectionResultsOperator(
        task_id=f"read_dlp_results",
        project_id=dlp_results_table_ref.project,
        dataset_id=dlp_results_table_ref.dataset_id,
        table_id=dlp_results_table_ref.table_id,
        do_xcom_push=True,
        min_match_count=table_dlp_config.get_min_match_count(source_dlp_config),
        task_group=taskgroup,
        dag=dag)

    # 5. Update policy tags
    update_tags_task = PythonOperator(
        task_id='update_bq_policy_tags',
        python_callable=update_bq_policy_tags,  # <--- PYTHON LIBRARY THAT COPIES FILES FROM SRC TO DEST
        task_group=taskgroup,
        dag=dag,
        templates_dict={
            "dlp_results": "{{ti.xcom_pull(task_ids='dlp_scan_table.read_dlp_results')}}",
        },
        op_kwargs={
            'project_id': project_id,
            'dataset_id': table_ref.dataset_id,
            'table_id': table_ref.table_id,
            'policy_tag_config': source_dlp_config.policy_tag_config,
            'task_ids': 'dlp_scan_table.read_dlp_results'
        },
        provide_context=True
    )

    delete_dlp_results >> scan >> read_results >> update_tags_task

    return taskgroup


def update_bq_policy_tags(project_id, dataset_id, table_id, policy_tag_config, gcp_conn_id='google_cloud_default',
                          delegate_to=None, **context):
    dlp_results = context['templates_dict']['dlp_results']
    logging.info(f"dlp_results{project_id}.{dataset_id}.{table_id} to {dlp_results}")

    hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to,
    )

    schema_fields_updates = results_to_bq_policy_tags(project_id, dlp_results, policy_tag_config)

    logging.info(f"updating policy tags for {project_id}.{dataset_id}.{table_id} to {schema_fields_updates}")
    hook.update_table_schema(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields_updates=schema_fields_updates,
        include_policy_tags=False  # Seem to work fine with False.
        # When set to True it tries to add a policy tags instead of replacing
    )
