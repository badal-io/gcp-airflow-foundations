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
"""This module contains Google Dataflow operators."""
import copy
import re
import warnings
from contextlib import ExitStack
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    process_line_and_extract_dataflow_job_id_callback,
)
from gcp_airflow_foundations.operators.api.hooks.dataflow_hooks import DataflowHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.dataflow import DataflowJobLink
from airflow.version import version

if TYPE_CHECKING:
    from airflow.utils.context import Context

from gcp_airflow_foundations.operators.api.triggers.dataflow_trigger import DataflowJobStatusTrigger


class CheckJobRunning(Enum):
    """
    Helper enum for choosing what to do if job is already running
    IgnoreJob - do not check if running
    FinishIfRunning - finish current dag run with no action
    WaitForRun - wait for job to finish and then continue with new job
    """

    IgnoreJob = 1
    FinishIfRunning = 2
    WaitForRun = 3


class DataflowConfiguration:

    template_fields: Sequence[str] = ("job_name", "location")

    def __init__(
        self,
        *,
        job_name: str = "{{task.task_id}}",
        append_job_name: bool = True,
        project_id: Optional[str] = None,
        location: Optional[str] = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
        multiple_jobs: Optional[bool] = None,
        check_if_running: CheckJobRunning = CheckJobRunning.WaitForRun,
        service_account: Optional[str] = None,
    ) -> None:
        self.job_name = job_name
        self.append_job_name = append_job_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = False
        self.multiple_jobs = multiple_jobs
        self.check_if_running = check_if_running
        self.service_account = service_account


class DataflowTemplatedJobStartOperatorAsync(BaseOperator):
    template_fields: Sequence[str] = (
        "template",
        "job_name",
        "options",
        "parameters",
        "project_id",
        "location",
        "gcp_conn_id",
        "impersonation_chain",
        "environment",
        "dataflow_default_options",
    )
    ui_color = "#0273d4"
    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        *,
        template: str,
        job_name: str = "{{task.task_id}}",
        options: Optional[Dict[str, Any]] = None,
        dataflow_default_options: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, str]] = None,
        project_id: Optional[str] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10, 
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        environment: Optional[Dict] = None,
        cancel_timeout: Optional[int] = 10 * 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.template = template
        self.job_name = job_name
        self.options = options or {}
        self.dataflow_default_options = dataflow_default_options or {}
        self.parameters = parameters or {}
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.poll_sleep = poll_sleep
        self.job = None
        self.hook: Optional[DataflowHook] = None
        self.impersonation_chain = impersonation_chain
        self.environment = environment
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = False

    def execute(self, context: 'Context') -> dict:
        self.hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
        )

        def set_current_job(current_job):
            self.job = current_job
            DataflowJobLink.persist(self, context, self.project_id, self.location, self.job.get("id"))

        options = self.dataflow_default_options
        options.update(self.options)
        job, job_id = self.hook.start_template_dataflow(
            job_name=self.job_name,
            variables=options,
            parameters=self.parameters,
            dataflow_template=self.template,
            on_new_job_callback=set_current_job,
            project_id=self.project_id,
            location=self.location,
            environment=self.environment,
        )

        self.defer(
            trigger=DataflowJobStatusTrigger(job_id=job_id),
            method_name="execute_complete", 
        )

    def execute_complete(self, context, event=None):
        """Callback for when the trigger fires - returns immediately."""
        if event:
            if event["status"] == "success":
                self.log.info("Job %s completed successfully.", event["job_id"])
                return event["job_id"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
