#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import os
from typing import Any, Callable, FrozenSet, Iterable, Optional, Union

from sqlalchemy import func

from airflow.exceptions import AirflowException
from airflow.models import BaseOperatorLink, DagBag, DagModel, DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.session import provide_session
from airflow.utils.state import State


class CustomExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG or a task in a different DAG to complete for a
    specific execution_date
    :param external_ids_map: A map whose keys are the DAG IDs you 
        want to wait for and the values are a list of the task IDs you want
        to wait for
    :type external_ids_map: dict

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

    template_fields = ['external_ids_map']
    ui_color = '#19647e'

    def __init__(
        self,
        *,
        external_ids_map: dict,
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

        # TO-DO Add validation statements here
       
        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_delta` or `execution_date_fn` may '
                'be provided to ExternalTaskSensor; not both.'
            )

        self.execution_delta = execution_delta
        self.external_ids_map = external_ids_map

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        else:
            dttm = context['execution_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ','.join(dt.isoformat() for dt in dttm_filter)

        count_allowed = self.get_count(dttm_filter, session, self.allowed_states)

        count_failed = -1
        if self.failed_states:
            count_failed = self.get_count(dttm_filter, session, self.failed_states)

        if count_failed == len(dttm_filter):
            pass

        return count_allowed == len(dttm_filter)

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

        expected_tasks_count = sum([len(self.external_ids_map[i]) for i in self.external_ids_map])
        count = 0

        for external_dag_id in self.external_ids_map:
            external_task_ids = self.external_ids_map[external_dag_id]

            if len(external_task_ids) > 0:
                count += (
                    session.query(func.count())  # .count() is inefficient
                    .filter(
                        TI.dag_id == external_dag_id,
                        TI.task_id.in_(external_task_ids),
                        TI.state.in_(states),
                        TI.execution_date.in_(dttm_filter),
                    )
                    .scalar()
                )
        
        external_dag_ids = [i for i in self.external_ids_map if len(self.external_ids_map[i]) == 0]
        count += (
            session.query(func.count())
            .filter(
                DR.dag_id.in_(external_dag_ids),
                DR.state.in_(states),
                DR.execution_date.in_(dttm_filter),
            )
            .scalar()
        )

        self.log.info(
            'Count is %s, expected dag is %s, expected tasks is %s',
            count,
            len(external_dag_ids),
            expected_tasks_count
        )
            
        return count / (expected_tasks_count + len(external_dag_ids))
