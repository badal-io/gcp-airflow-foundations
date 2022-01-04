
from gcp_airflow_foundations.base_class.dlp_source_config import DlpSourceConfig
from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.operators.gcp.dlp.dlp_job_helpers import job_trigger
from gcp_airflow_foundations.operators.gcp.dlp.get_dlp_bq_inspection_results_operator import DlpBQInspectionResultsOperator
from airflow.utils.task_group import TaskGroup


def dlp_to_datacatalog_builder(
        project_id: str,
        table_id: str,
        dataset_id: str,
        source_dlp_config: DlpSourceConfig,
        table_dlp_config: DlpTableConfig,
        dag) -> TaskGroup:


    dlp_results_dataset_id = source_dlp_config.results_dataset_id

    dlp_template_name = table_dlp_config.get_template_name(source_dlp_config)

    dlp_results_table_id = "{}_{}_dlp_results".format(dataset_id, table_id)

    """
    Method for returning a Task Group for
     1) TODO:
    """

    taskgroup = TaskGroup(group_id="dlp_scan_table_taskgroup")

    #0 First delete the results table
    delete_dlp_results = BigQueryTableDeleteOperator(
        task_id="wait_for_dlp_results",
        project_id=project_id,
        dataset_id=dlp_results_dataset_id,
        table_id=dlp_results_table_id,
        task_group=taskgroup,
    )

    #1 Check if HDS table exists and if not create an empty table
    scan = CloudDLPCreateJobTriggerOperator(
        task_id="scan_table",
        project_id=project_id,
        job_trigger=job_trigger("test_job", dlp_template_name ,project_id, dataset_id, table_id, rows_limit_percent,
                                project_id, dlp_results_dataset_id, dlp_results_table_id),
        trigger_id="airflow_scan",
        task_group=taskgroup,
        dag=dag
    )

    #2 Wait for results
    wait_for_results = BigQueryTableExistenceSensor(
        task_id="wait_for_dlp_results",
        project_id=project_id,
        dataset_id=dlp_results_dataset_id,
        table_id=dlp_results_table_id,
        task_group=taskgroup,
        dag=dag
    )

    #3. Read results
    read_reasults = DlpBQInspectionResultsOperator(
        task_id=f"read_dlp_results",
        project_id=project_id,
        dataset_id=dlp_results_dataset_id,
        table_id=dlp_results_table_id,
        min_match_count=table_dlp_config.get_min_match_count(source_dlp_config),
        task_group=taskgroup,
        dag=dag
    )

    scan >> wait_for_results # >> upload_results_to_datacatalog

    return taskgroup



