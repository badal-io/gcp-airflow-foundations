import datetime
import os
from typing import Any, Callable, FrozenSet, Iterable, Optional, Union
import re

from sqlalchemy import func

from airflow.exceptions import AirflowException
from airflow.models import BaseOperatorLink, DagBag, DagModel, DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.session import provide_session
from airflow.utils.state import State


class TableIngestionSensor(BaseSensorOperator):
    """
    Waits for table ingestion DAGs to complete for a
    specific execution_date

    :param external_source_tables: A map whose keys are the sources 
        to wait for and the values are a list of the tables for each source
    :type external_source_tables: dict
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :type allowed_states: Iterable
    :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
    :type failed_states: Iterable
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: Optional[datetime.timedelta]
    """

    template_fields = ['external_source_tables']
    ui_color = '#19647e'

    def __init__(
        self,
        *,
        external_source_tables: dict,
        allowed_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        execution_delta: Optional[datetime.timedelta] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.allowed_states = list(allowed_states) if allowed_states else [State.SUCCESS]
        self.failed_states = list(failed_states) if failed_states else []

        total_states = self.allowed_states + self.failed_states
        total_states = set(total_states)

        if set(self.failed_states).intersection(set(self.allowed_states)):
            raise AirflowException(
                f"Duplicate values provided as allowed "
                f"`{self.allowed_states}` and failed states `{self.failed_states}`"
            )
       
        self.execution_delta = execution_delta
        self.external_dag_ids = self.get_external_dag_ids()

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        else:
            dttm = context['execution_date']

        dttm_filter = [dttm]
        serialized_dttm_filter = ','.join(dt.isoformat() for dt in dttm_filter)

        count_allowed = self.get_count(dttm_filter, session, self.allowed_states)

        count_failed = -1
        if self.failed_states:
            count_failed = self.get_count(dttm_filter, session, self.failed_states)

        if count_failed == len(dttm_filter):
            pass # TO-DO: define behaviour if any of the dependent DAGs have failed. Maybe provide list of essential tables?

        return count_allowed == 1

    def get_count(self, dttm_filter, session, states) -> int:
        """
        Get the count of records against dttm filter and states
        :param dttm_filter: date time filter for execution date
        :type dttm_filter: list
        :param session: airflow session object
        :type session: SASession
        :param states: task or dag states
        :type states: list
        :return: count of record against the filters
        """
        TI = TaskInstance
        DR = DagRun
        
        expected_count = len(self.external_dag_ids)
        count = (
            session.query(func.count())
            .filter(
                DR.dag_id.in_(self.external_dag_ids),
                DR.state.in_(states),
                DR.execution_date.in_(dttm_filter),
            )
            .scalar()
        )

        self.log.info(
            'Current count of completed tasks is %s. The expected DAG count is %s',
            count,
            expected_count
        )
            
        return count / expected_count

    @provide_session
    def get_external_dag_ids(self, context, session=None) -> list:
        """
        Retrieve a list of external DAG IDs based on the provided source & table combinations
        """
        schedule_interval = context['dag'].schedule_interval

        external_dag_ids = []

        # Query all active dags
        query = session.query(DagModel).filter(DagModel.is_active==True).all()

        if len(query) == 0:
            raise AirflowException(f'No active dags found')

        schedule_map = {}
        source_dag_map = {}
        for dag in query:
            dag_id = dag.dag_id
            schedule_map[dag_id] = dag.schedule_interval
            source = dag_id.split('_')[0]
            if source in source_dag_map:
                source_dag_map[source] = source_dag_map[source].append(dag_id)
            else:
                source_dag_map[source] = [dag_id]
            
        for source, tables in self.external_source_tables.items():
            source_dags = source_dag_map[source]

            if not source_dags:
                raise AirflowException(f'No active dags found for source {source}')
            
            for regex in tables:
                table_dags = [
                    dag for dag in source_dags if re.match(regex, dag.split('_')[1]) and schedule_interval == schedule_map[dag]
                ]

                if not table_dags:
                        raise AirflowException(f'No active dags found for source {source} and table {table}')

                external_dag_ids.extend(set(table_dags))
                        
        return external_dag_ids