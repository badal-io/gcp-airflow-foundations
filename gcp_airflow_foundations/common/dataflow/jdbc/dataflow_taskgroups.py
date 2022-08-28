from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from gcp_airflow_foundations.operators.api.operators.dataflow_operator import DataflowTemplatedJobStartOperatorAsync


def dataflow_taskgroup_builder(
    query_schema,
    dataflow_job_params,
    dataflow_table_params,
    destination_table,
    destination_schema_table,
    table_name,
    system_name,
    create_job_params,
    run_dataflow_job,
    create_table,
    ingest_metadata,
    table_type_casts,
    ingestion_type
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
                   "owner": dataflow_job_params["database_owner"],
                   "ingestion_type": ingestion_type,
                   "table_params": dataflow_table_params},
        python_callable=create_job_params,
        task_group=taskgroup,
    )

    use_deferrable_operator = True
    if use_deferrable_operator:
        xcom_task_id = f"{table_name}.dataflow_taskgroup.create_job_parameters"
        trigger_dataflow_job = DataflowTemplatedJobStartOperatorAsync(
            task_id="run_dataflow_job_to_bq",
            dataflow_default_options="{{ ti.xcom_pull(key='dataflow_default_options', task_ids=f'{xcom_task_id}') }}",
            parameters="{{ ti.xcom_pull(key='parameters', task_ids=f'{xcom_task_id}') }}",
            job_name=f"{system_name.lower()}-upload-{table_name.lower()}-to-bq".replace("_", "-"),
            template=dataflow_job_params["template_path"]
        )
    else:
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

        create_table = PythonOperator(
            task_id="create_table_if_needed",
            op_kwargs={"destination_table": destination_table,
                       "schema_table": destination_schema_table,
                       "source_table": table_name,
                       "table_type_casts": table_type_casts},
            python_callable=create_table,
            task_group=taskgroup
        )

        if ingest_metadata:
            schema_task_sensor = ExternalTaskSensor(
                task_id="check_bq_schema_updated",
                external_dag_id=f"{system_name}_upload_schema",
                task_group=taskgroup,
            )
            schema_task_sensor >> create_job_parameters >> create_table >> trigger_dataflow_job
        else:
            create_job_parameters >> create_table >> trigger_dataflow_job

    else:
        create_job_parameters >> trigger_dataflow_job

    return taskgroup
