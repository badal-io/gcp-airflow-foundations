from google.cloud.bigquery.table import TableReference


# https://cloud.google.com/dlp/docs/reference/rest/v2/projects.jobTriggers#JobTrigger
def build_job_trigger(
    display_name: str,
    template_name: str,
    table_ref: TableReference,
    rows_limit_percent: int,
    results_table_ref: TableReference,
    recurrence: str,
):
    schedule = {"recurrence_period_duration": {"seconds": 30 * 60 * 60 * 24}}
    return {
        "display_name": display_name,
        "description": "DLP scan for {}.{} triggered by Airflow".format(
            table_ref.dataset_id, table_ref.table_id
        ),
        "inspect_job": build_inspect_job_config(
            template_name, table_ref, rows_limit_percent, results_table_ref
        ),
        "triggers": [{"schedule": schedule}],
        "status": "HEALTHY",
    }


def build_inspect_job_config(
    template_name, table_ref, rows_limit_percent, results_table_ref
):
    """ Create an inspection job config: https://cloud.google.com/dlp/docs/creating-job-triggers"""
    return {
        "storage_config": __bq_storage_inspect_job_config(
            table_ref, rows_limit_percent
        ),
        "inspect_template_name": template_name,
        "actions": [__action_save_to_bq(results_table_ref)],
    }


def __bq_storage_inspect_job_config(table_ref, rows_limit_percent):
    return {
        "big_query_options": {
            "table_reference": {
                "project_id": table_ref.project,
                "dataset_id": table_ref.dataset_id,
                "table_id": table_ref.table_id,
            },
            "rows_limit_percent": rows_limit_percent,
            "sample_method": "RANDOM_START",
        }
    }


def __action_save_to_bq(table_ref):
    return {
        "save_findings": {
            "output_config": {
                "output_schema": "BIG_QUERY_COLUMNS",
                "table": {
                    "project_id": table_ref.project,
                    "dataset_id": table_ref.dataset_id,
                    "table_id": table_ref.table_id,
                },
            }
        }
    }
