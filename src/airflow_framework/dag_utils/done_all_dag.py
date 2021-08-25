from typing import List

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models.dag import DAG
import logging


def wait_for_all_and_notify(
    dags: List[DAG],
    dag_id: str,
    dagrun_timeout,
    task_id_to_wait_for: str,
    notify_email: str,
    subject: str,
    default_task_args,
):
    def getTaskToWaitFor(dag: DAG):
        return ExternalTaskSensor(
            task_id=f"wait_for_{dag.dag_id}",
            external_dag_id=dag.dag_id,
            external_task_id=task_id_to_wait_for,
            mode="reschedule",
        )

    dag = DAG(
        dag_id=dag_id,
        description="",
        schedule_interval="@daily",
        catchup=True,
        dagrun_timeout=dagrun_timeout,
        default_args=default_task_args,
    )
    with dag:
        tasks_to_wait_for: List[ExternalTaskSensor] = list(map(getTaskToWaitFor, dags))

        done_all = DummyOperator(
            task_id=f"done_{dag_id}", default_args=default_task_args
        )

        done_email = EmailOperator(
            default_args=default_task_args,
            task_id=f"notify_{dag_id}",
            to=notify_email,
            subject=subject,
            html_content="""
               <p> The following dags have successfully finished</p>
              {% for dag  in dags %}
                <tr>
                    <td>{{ dag.dag_id }}</td>
                    <td>{{ dag.start_date }}</td>
                </tr>
             {% endfor %}
            """,
        )
        tasks_to_wait_for >> done_all >> done_email

    return dag
