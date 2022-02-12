from typing import Dict, Iterable, Union

from airflow.operators.branch import BaseBranchOperator
from airflow.utils import timezone
from croniter import croniter
from datetime import datetime, timedelta

import logging


class BranchOnCronOperator(BaseBranchOperator):
    """
    Branches into one of two lists of tasks depending on the current day and cron expressions
    :param follow_task_ids_if_true: task id or task ids to follow if criteria met
    :type follow_task_ids_if_true: str or list[str]
    :param follow_task_ids_if_false: task id or task ids to follow if criteria does not met
    :type follow_task_ids_if_false: str or list[str]
    :param cron_expression: cron expression to evaluate
    :type cron_expression: str
    :param use_task_execution_day: If ``True``, uses task's execution day to compare
        with is_today. Execution Date is Useful for backfilling.
        If ``False``, uses system's day of the week.
    :type use_task_execution_day: bool
    :param run_on_first_execution: If ``True``, returns true on  first time this task is run
    :type run_on_first_execution: bool
    """

    def __init__(
        self,
        *,
        follow_task_ids_if_true: Union[str, Iterable[str]],
        follow_task_ids_if_false: Union[str, Iterable[str]],
        cron_expression: str,
        use_task_execution_day: bool = True,
        run_on_first_execution: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.follow_task_ids_if_true = follow_task_ids_if_true
        self.follow_task_ids_if_false = follow_task_ids_if_false
        self.cron_expression = cron_expression
        self.use_task_execution_day = use_task_execution_day
        self.run_on_first_execution = run_on_first_execution

        if not croniter.is_valid(cron_expression):
            raise TypeError("Unsupported cron expression {}".format(cron_expression))

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        logging.info(f"context {context}")
        ti = context["ti"]
        logging.info(f"ti {ti}")

        if self.use_task_execution_day:
            now = context["execution_date"]
        else:
            now = timezone.make_naive(timezone.utcnow(), self.dag.timezone)

        first_run = ti.get_previous_ti() is None

        if first_run and self.run_on_first_execution:
            return self.follow_task_ids_if_true
        elif self.match(self.cron_expression, now):
            return self.follow_task_ids_if_true
        else:
            return self.follow_task_ids_if_false

    def match(self, cron_expression, date):
        iter = croniter(cron_expression, date)
        iter.get_prev()
        return iter.get_next(datetime) == date
