
# https://cloud.google.com/dlp/docs/reference/rest/v2/projects.jobTriggers#JobTrigger
def job_trigger(display_name, template_name, project_id, dataset_id, table_id, rows_limit_percent, results_project_id, results_dataset_id, results_table_id):
    return {
        "display_name": display_name,
        "description": "DLP scan for {}.{} triggered by Airflow".format(dataset_id, table_id),
        "inspect_job": __bq_inspect_job_config(template_name, project_id, dataset_id, table_id, rows_limit_percent,
                                               results_project_id, results_dataset_id, results_table_id),
        "triggers": [__manual_job_trigger()],
        "status": "HEALTHY",
    }

def __manual_job_trigger():
    return {"manual": {}}

def __scheduled_job_trigger_recurrencing(seconds):
    return {"schedule": {"recurrence_period_duration": {"seconds": seconds}}}

def __bq_inspect_job_config(template_name, project_id, dataset_id, table_id, rows_limit_percent, results_project_id, results__dataset_id, results_table_id):
    return {
        "storage_config": __bq_storage_inspect_job_config(project_id, dataset_id, table_id, rows_limit_percent),
        "inspect_template_name": template_name,
        "actions": [__action_save_to_bq(results_project_id, results__dataset_id, results_table_id)]
    }

def __bq_storage_inspect_job_config(project_id, dataset_id, table_id, rows_limit_percent):
    return {
            "big_query_options": {
                "table_reference": {"project_id": project_id, "dataset_id": dataset_id, "table_id": table_id },
                "rows_limit_percent": rows_limit_percent,
                "sample_method": "RANDOM_START",
            }
    }

def __action_save_to_bq(project_id, dataset_id, table_id):
    return {
        "save_findings": {
            "output_config": {
                "output_schema": "BIG_QUERY_COLUMNS",
                "table": {"project_id": project_id, "dataset_id": dataset_id, "table_id": table_id }

            }
        }
    }
