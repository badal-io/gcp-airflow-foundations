from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from gcp_airflow_foundations.operators.api.operators.dataflow_operator import DataflowTemplatedJobStartOperatorAsync
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor


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

    dataflow_sensor = DataflowJobStatusSensor(
        task_id="wait_for_dataflow_job",
        job_id="{{ ti.xcom_pull(key='job_id', task_ids='" + table_name + ".dataflow_taskgroup.run_dataflow_job_to_bq') }}",
        location="US"
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
            schema_task_sensor >> create_job_parameters >> create_table >> trigger_dataflow_job >> dataflow_sensor
        else:
            create_job_parameters >> create_table >> trigger_dataflow_job >> dataflow_sensor

    else:
        create_job_parameters >> trigger_dataflow_job

    return taskgroup
