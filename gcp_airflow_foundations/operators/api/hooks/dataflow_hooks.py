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
"""This module contains a Google Dataflow Hook."""
import functools
import json
import re
import shlex
import subprocess
import time
import uuid
import warnings
from copy import deepcopy
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Set, TypeVar, Union, cast

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType, beam_options_to_args
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.dataflow import _DataflowJobsController
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timeout import timeout

# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = "us-central1"


JOB_ID_PATTERN = re.compile(
    r"Submitted job: (?P<job_id_java>.*)|Created job with id: \[(?P<job_id_python>.*)\]"
)

T = TypeVar("T", bound=Callable)


def process_line_and_extract_dataflow_job_id_callback(
    on_new_job_id_callback: Optional[Callable[[str], None]]
) -> Callable[[str], None]:
    """
    Returns callback which triggers function passed as `on_new_job_id_callback` when Dataflow job_id is found.
    To be used for `process_line_callback` in
    :py:class:`~airflow.providers.apache.beam.hooks.beam.BeamCommandRunner`
    :param on_new_job_id_callback: Callback called when the job ID is known
    """

    def _process_line_and_extract_job_id(
        line: str,
        # on_new_job_id_callback: Optional[Callable[[str], None]]
    ) -> None:
        # Job id info: https://goo.gl/SE29y9.
        matched_job = JOB_ID_PATTERN.search(line)
        if matched_job:
            job_id = matched_job.group("job_id_java") or matched_job.group("job_id_python")
            if on_new_job_id_callback:
                on_new_job_id_callback(job_id)

    def wrap(line: str):
        return _process_line_and_extract_job_id(line)

    return wrap


def _fallback_variable_parameter(parameter_name: str, variable_key_name: str) -> Callable[[T], T]:
    def _wrapper(func: T) -> T:
        """
        Decorator that provides fallback for location from `region` key in `variables` parameters.
        :param func: function to wrap
        :return: result of the function call
        """

        @functools.wraps(func)
        def inner_wrapper(self: "DataflowHook", *args, **kwargs):
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            parameter_location = kwargs.get(parameter_name)
            variables_location = kwargs.get("variables", {}).get(variable_key_name)

            if parameter_location and variables_location:
                raise AirflowException(
                    f"The mutually exclusive parameter `{parameter_name}` and `{variable_key_name}` key "
                    f"in `variables` parameter are both present. Please remove one."
                )
            if parameter_location or variables_location:
                kwargs[parameter_name] = parameter_location or variables_location
            if variables_location:
                copy_variables = deepcopy(kwargs["variables"])
                del copy_variables[variable_key_name]
                kwargs["variables"] = copy_variables

            return func(self, *args, **kwargs)

        return cast(T, inner_wrapper)

    return _wrapper


_fallback_to_location_from_variables = _fallback_variable_parameter("location", "region")
_fallback_to_project_id_from_variables = _fallback_variable_parameter("project_id", "project")


class DataflowJobStatus:
    """
    Helper class with Dataflow job statuses.
    Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    """

    JOB_STATE_DONE = "JOB_STATE_DONE"
    JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN"
    JOB_STATE_STOPPED = "JOB_STATE_STOPPED"
    JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
    JOB_STATE_FAILED = "JOB_STATE_FAILED"
    JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
    JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
    JOB_STATE_DRAINING = "JOB_STATE_DRAINING"
    JOB_STATE_DRAINED = "JOB_STATE_DRAINED"
    JOB_STATE_PENDING = "JOB_STATE_PENDING"
    JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING"
    JOB_STATE_QUEUED = "JOB_STATE_QUEUED"
    FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
    SUCCEEDED_END_STATES = {JOB_STATE_DONE, JOB_STATE_UPDATED, JOB_STATE_DRAINED}
    TERMINAL_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES
    AWAITING_STATES = {
        JOB_STATE_RUNNING,
        JOB_STATE_PENDING,
        JOB_STATE_QUEUED,
        JOB_STATE_CANCELLING,
        JOB_STATE_DRAINING,
        JOB_STATE_STOPPED,
    }


class DataflowJobType:
    """Helper class with Dataflow job types."""

    JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN"
    JOB_TYPE_BATCH = "JOB_TYPE_BATCH"
    JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"


class _DataflowJobsControllerAsync(LoggingMixin):
    """
    Interface for communication with Google API.
    It's not use Apache Beam, but only Google Dataflow API.
    :param dataflow: Discovery resource
    :param project_number: The Google Cloud Project ID.
    :param location: Job location.
    :param poll_sleep: The status refresh rate for pending operations.
    :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
    :param job_id: ID of a single job.
    :param num_retries: Maximum number of retries in case of connection problems.
    :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
        not by specific job ID, then actions will be performed on all matching jobs.
    :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
        instead of canceling.
    :param cancel_timeout: wait time in seconds for successful job canceling
    :param wait_until_finished: If True, wait for the end of pipeline execution before exiting. If False,
        it only submits job and check once is job not in terminal state.
        The default behavior depends on the type of pipeline:
        * for the streaming pipeline, wait for jobs to start,
        * for the batch pipeline, wait for the jobs to complete.
    """

    def __init__(
        self,
        dataflow: Any,
        project_number: str,
        location: str,
        poll_sleep: int = 10,
        name: Optional[str] = None,
        job_id: Optional[str] = None,
        num_retries: int = 0,
        multiple_jobs: bool = False,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
    ) -> None:

        super().__init__()
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._multiple_jobs = multiple_jobs
        self._job_id = job_id
        self._num_retries = num_retries
        self._poll_sleep = poll_sleep
        self._cancel_timeout = cancel_timeout
        self._jobs: Optional[List[dict]] = None
        self.drain_pipeline = drain_pipeline
        self._wait_until_finished = wait_until_finished

    async def _get_current_jobs(self) -> List[dict]:
        """
        Helper method to get list of jobs that start with job name or id
        :return: list of jobs including id's
        :rtype: list
        """
        if not self._multiple_jobs and self._job_id:
            return [await self.fetch_job_by_id(self._job_id)]
        elif self._job_name:
            jobs = await self._fetch_jobs_by_prefix_name(self._job_name.lower())
            if len(jobs) == 1:
                self._job_id = jobs[0]["id"]
            return jobs
        else:
            raise Exception("Missing both dataflow job ID and name.")

    async def fetch_job_by_id(self, job_id: str) -> dict:
        """
        Helper method to fetch the job with the specified Job ID.
        :param job_id: Job ID to get.
        :return: the Job
        :rtype: dict
        """
        return (
            self._dataflow.projects()
            .locations()
            .jobs()
            .get(
                projectId=self._project_number,
                location=self._job_location,
                jobId=job_id,
            )
            .execute(num_retries=self._num_retries)
        )

    async def _fetch_all_jobs(self) -> List[dict]:
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .list(projectId=self._project_number, location=self._job_location)
        )
        all_jobs: List[dict] = []
        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            jobs = response.get("jobs")
            if jobs is None:
                break
            all_jobs.extend(jobs)

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .list_next(previous_request=request, previous_response=response)
            )
        return all_jobs

    async def _fetch_jobs_by_prefix_name(self, prefix_name: str) -> List[dict]:
        jobs = await self._fetch_all_jobs()
        jobs = [job for job in jobs if job["name"].startswith(prefix_name)]
        return jobs

    async def _refresh_jobs(self) -> None:
        """
        Helper method to get all jobs by name
        :return: jobs
        :rtype: list
        """
        self._jobs = await self._get_current_jobs()

        if self._jobs:
            for job in self._jobs:
                self.log.info(
                    "Google Cloud DataFlow job %s is state: %s",
                    job["name"],
                    job["currentState"],
                )
        else:
            self.log.info("Google Cloud DataFlow job not available yet..")

    async def _check_dataflow_job_state(self, job) -> bool:
        """
        Helper method to check the state of one job in dataflow for this task
        if job failed raise exception
        :return: True if job is done.
        :rtype: bool
        :raise: Exception
        """
        if self._wait_until_finished is None:
            wait_for_running = job.get('type') == DataflowJobType.JOB_TYPE_STREAMING
        else:
            wait_for_running = not self._wait_until_finished

        if job['currentState'] == DataflowJobStatus.JOB_STATE_DONE:
            return True
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_FAILED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} has failed.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_CANCELLED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was cancelled.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_DRAINED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was drained.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_UPDATED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was updated.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_RUNNING and wait_for_running:
            return True
        elif job['currentState'] in DataflowJobStatus.AWAITING_STATES:
            return self._wait_until_finished is False
        self.log.debug("Current job: %s", str(job))
        raise Exception(f"Google Cloud Dataflow job {job['name']} was unknown state: {job['currentState']}")

    async def wait_for_done(self) -> None:
        """Helper method to wait for result of submitted job."""
        self.log.info("Start waiting for done.")
        await self._refresh_jobs()
        while self._jobs and not all(await self._check_dataflow_job_state(job) for job in self._jobs):
            self.log.info("Waiting for done. Sleep %s s", self._poll_sleep)
            time.sleep(self._poll_sleep)
            await self._refresh_jobs()

    async def get_jobs(self, refresh: bool = False) -> List[dict]:
        """
        Returns Dataflow jobs.
        :param refresh: Forces the latest data to be fetched.
        :return: list of jobs
        :rtype: list
        """
        if not self._jobs or refresh:
            await self._refresh_jobs()
        if not self._jobs:
            raise ValueError("Could not read _jobs")

        return self._jobs

class DataflowAsyncHook(GoogleBaseHook):
    """
    Hook for Google Dataflow.
    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
    ) -> None:
        self.poll_sleep = poll_sleep
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job_id: Optional[str] = None
        self.beam_hook = BeamHook(BeamRunnerType.DataflowRunner)
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    async def get_conn(self) -> build:
        """Returns a Google Cloud Dataflow service object."""
        http_authorized = self._authorize()
        built = build("dataflow", "v1b3", http=http_authorized, cache_discovery=False)
        return built

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    async def start_template_dataflow(
        self,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        on_new_job_callback: Optional[Callable[[dict], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: Optional[dict] = None,
    ) -> dict:
        """
        Starts Dataflow template job.
        :param job_name: The name of the job.
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.
            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
        :param parameters: Parameters for the template
        :param dataflow_template: GCS path to the template.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param on_new_job_id_callback: (Deprecated) Callback called when the Job is known.
        :param on_new_job_callback: Callback called when the Job is known.
        :param location: Job location.
            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)

        environment = environment or {}
        # available keys for runtime environment are listed here:
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment_keys = [
            "numWorkers",
            "maxWorkers",
            "zone",
            "serviceAccountEmail",
            "tempLocation",
            "bypassTempDirValidation",
            "machineType",
            "additionalExperiments",
            "network",
            "subnetwork",
            "additionalUserLabels",
            "kmsKeyName",
            "ipConfiguration",
            "workerRegion",
            "workerZone",
        ]

        for key in variables:
            if key in environment_keys:
                if key in environment:
                    self.log.warning(
                        "'%s' parameter in 'variables' will override of "
                        "the same one passed in 'environment'!",
                        key,
                    )
                environment.update({key: variables[key]})

        service = await self.get_conn()

        request = (
            service.projects()
            .locations()
            .templates()
            .launch(
                projectId=project_id,
                location=location,
                gcsPath=dataflow_template,
                body={
                    "jobName": name,
                    "parameters": parameters,
                    "environment": environment,
                },
            )
        )
        response = request.execute(num_retries=self.num_retries)

        job = response["job"]

        if on_new_job_id_callback:
            warnings.warn(
                "on_new_job_id_callback is Deprecated. Please start using on_new_job_callback",
                DeprecationWarning,
                stacklevel=3,
            )
            on_new_job_id_callback(job.get("id"))

        if on_new_job_callback:
            on_new_job_callback(job)

        jobs_controller = _DataflowJobsControllerAsync(
            dataflow=await self.get_conn(),
            project_number=project_id,
            name=name,
            job_id=job["id"],
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=False,
        )
        await jobs_controller.wait_for_done()
        return response["job"], job["id"]

    @staticmethod
    def build_dataflow_job_name(job_name: str, append_job_name: bool = True) -> str:
        """Builds Dataflow job name."""
        base_job_name = str(job_name).replace("_", "-")

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                f"Invalid job_name ({base_job_name}); the name must consist of only the characters "
                f"[-a-z0-9], starting with a letter and ending with a letter or number "
            )

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Gets the job with the specified Job ID.
        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the Job
        :rtype: dict
        """
        jobs_controller = _DataflowJobsControllerAsync(
            dataflow=await self.get_conn(),
            project_number=project_id,
            location=location,
        )
        job = await jobs_controller.fetch_job_by_id(job_id)
        return job

class DataflowHook(GoogleBaseHook):
    """
    Hook for Google Dataflow.
    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
    ) -> None:
        self.poll_sleep = poll_sleep
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job_id: Optional[str] = None
        self.beam_hook = BeamHook(BeamRunnerType.DataflowRunner)
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> build:
        """Returns a Google Cloud Dataflow service object."""
        http_authorized = self._authorize()
        return build("dataflow", "v1b3", http=http_authorized, cache_discovery=False)
    
    @staticmethod
    def build_dataflow_job_name(job_name: str, append_job_name: bool = True) -> str:
        """Builds Dataflow job name."""
        base_job_name = str(job_name).replace("_", "-")

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                f"Invalid job_name ({base_job_name}); the name must consist of only the characters "
                f"[-a-z0-9], starting with a letter and ending with a letter or number "
            )

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_template_dataflow(
        self,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        on_new_job_callback: Optional[Callable[[dict], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: Optional[dict] = None,
    ) -> dict:
        """
        Starts Dataflow template job.
        :param job_name: The name of the job.
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.
            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
        :param parameters: Parameters fot the template
        :param dataflow_template: GCS path to the template.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param on_new_job_id_callback: (Deprecated) Callback called when the Job is known.
        :param on_new_job_callback: Callback called when the Job is known.
        :param location: Job location.
            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)

        environment = environment or {}
        # available keys for runtime environment are listed here:
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment_keys = [
            "numWorkers",
            "maxWorkers",
            "zone",
            "serviceAccountEmail",
            "tempLocation",
            "bypassTempDirValidation",
            "machineType",
            "additionalExperiments",
            "network",
            "subnetwork",
            "additionalUserLabels",
            "kmsKeyName",
            "ipConfiguration",
            "workerRegion",
            "workerZone",
        ]

        for key in variables:
            if key in environment_keys:
                if key in environment:
                    self.log.warning(
                        "'%s' parameter in 'variables' will override of "
                        "the same one passed in 'environment'!",
                        key,
                    )
                environment.update({key: variables[key]})

        service = self.get_conn()

        request = (
            service.projects()
            .locations()
            .templates()
            .launch(
                projectId=project_id,
                location=location,
                gcsPath=dataflow_template,
                body={
                    "jobName": name,
                    "parameters": parameters,
                    "environment": environment,
                },
            )
        )
        response = request.execute(num_retries=self.num_retries)

        job = response["job"]

        if on_new_job_id_callback:
            warnings.warn(
                "on_new_job_id_callback is Deprecated. Please start using on_new_job_callback",
                DeprecationWarning,
                stacklevel=3,
            )
            on_new_job_id_callback(job.get("id"))

        if on_new_job_callback:
            on_new_job_callback(job)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            job_id=job["id"],
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
        )
        jobs_controller.wait_for_done()
        return response["job"], job["id"]

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Gets the job with the specified Job ID.
        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the Job
        :rtype: dict
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_by_id(job_id)
