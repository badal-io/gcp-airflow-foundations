
import datetime
import logging

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils import timezone
from airflow.utils.context import Context
from dep.dataflow_hooks import DataflowAsyncHook
from typing import Any, Dict, Optional, Sequence, Tuple, Union

from airflow.exceptions import AirflowException

import asyncio
import datetime
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone

class DataflowJobSensorAsync(BaseSensorOperator):
    """
    Waits until the specified time of the day, freeing up a worker slot while
    it is waiting.
    :param target_time: time after which the job succeeds
    """

    def __init__(self, job_id, **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id, 
        self.dataflow_hook = DataflowAsyncHook()

    def execute(self, context: Context):
        self.defer(
            trigger=DataflowJobStatusTrigger(job_id=self.job_id),
            method_name="execute_complete", 
        )

    def execute_complete(self, context, event=None):
        """Callback for when the trigger fires - returns immediately."""
        return None

class DataflowJobStatusTrigger(BaseTrigger):
    """
    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.
    The provided datetime MUST be in UTC.
    """

    def __init__(self, job_id: str, polling_period_seconds: int = 30):
        super().__init__()
        self.job_id = job_id
        self.polling_period_seconds = polling_period_seconds
        self.hook = DataflowAsyncHook()

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "dep.dataflow_triggers.DataflowJobStatusTrigger", 
            {
                "job_id": self.job_id,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self):
        try:
            dataflow_hook = self.hook
            while True:
                job_status = await self._get_job_status(dataflow_hook)
                if "status" in job_status and job_status["status"] == "success":
                    yield TriggerEvent(job_status)
                elif "status" in job_status and job_status["status"] == "error":
                    yield TriggerEvent(job_status)
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def _get_job_status(self, hook: DataflowAsyncHook) -> Dict[str, str]:
        """Gets the status of the given job_id from Dataflow"""
        job = await hook.get_job(
            job_id=self.job_id,
            project_id="cogeco-edh-prod",
            location="us-central1"
        )
        job_status = job["currentState"]
        if job_status in ["JOB_STATE_FAILED", "JOB_STATE_STOPPED", "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED"]:
            return {"status": "error", "message": "Job Failed", "job_id": self.job_id}
        elif job_status == "JOB_STATE_DONE":
            return {
                "status": "success",
                "message": "Job completed successfully",
                "job_id": self.job_id,
            }
        return {"status": "pending", "message": "Job is in pending state", "job_id": self.job_id}