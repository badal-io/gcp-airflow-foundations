from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor


def dataflow_taskgroup_builder(
    query_schema,
    dataflow_job_params,
    destination_table,
    destination_schema_table,
    table_name,
    system_name,
    create_job_params,
    run_dataflow_job,
    create_table
) -> TaskGroup:

    """
    Method for returning a Task Group for JDBC->BQ Dataflow DAGS
        Either returns a taskgroup for 
            1) Table ingestion
            2) Schema ingestion
    """

    taskgroup = TaskGroup(group_id="dataflow_taskgroup")

    create_job_parameters = PythonOperator(
        task_id="create_job_parameters",
        op_kwargs={"config_params": dataflow_job_params,
                    "table_name": table_name,
                    "destination_table": destination_table,
                    "destination_schema_table": destination_schema_table,
                    "query_schema": query_schema,
                    "owner": dataflow_job_params["database_owner"]},
        python_callable=create_job_params,
        task_group=taskgroup,
    )

    trigger_dataflow_job = PythonOperator(
        task_id="run_dataflow_job_to_bq",
        op_kwargs={"template_path": dataflow_job_params["template_path"],
                    "system_name": system_name,
                    "table_name": table_name,
                    "query_schema": query_schema},
        python_callable=run_dataflow_job,
        task_group=taskgroup,
        pool=dataflow_job_params["connection_pool"]
    )

    if not query_schema:
        schema_task_sensor = ExternalTaskSensor(
            task_id="check_bq_schema_updated",
            external_dag_id=f"{system_name}_upload_schema",
            task_group=taskgroup,
        )

        create_table = PythonOperator(
            task_id="create_table_if_needed",
            op_kwargs={"destination_table": destination_table,
            "schema_table": destination_schema_table,
            "source_table": table_name},
            python_callable=create_table,
            task_group=taskgroup,
        )

        schema_task_sensor >> create_job_parameters >> create_table >> trigger_dataflow_job
    else:
        create_job_parameters >> trigger_dataflow_job

    return taskgroup
