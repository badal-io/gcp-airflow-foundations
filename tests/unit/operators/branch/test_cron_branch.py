import datetime
import unittest

from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

from gcp_airflow_foundations.operators.branch.BranchOnCronOperator import (
    BranchOnCronOperator,
)

DEFAULT_DATE = timezone.datetime(2020, 2, 5)  # Wednesday
INTERVAL = datetime.timedelta(hours=12)


class TestBranchDayOfWeekOperator(unittest.TestCase):
    """
    Tests for BranchDayOfWeekOperator
    """

    @classmethod
    def setUpClass(cls):

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            "branch_cron_test", start_date=DEFAULT_DATE, schedule_interval=INTERVAL
        )
        self.branch_1 = DummyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = DummyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def tearDown(self):

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def _assert_task_ids_match_states(self, dr, task_ids_to_states):
        """Helper that asserts task instances with a given id are in a given state"""
        tis = dr.get_task_instances()
        for ti in tis:
            try:
                expected_state = task_ids_to_states[ti.task_id]
            except KeyError:
                raise ValueError(f"Invalid task id {ti.task_id} found!")
            else:
                if isinstance(expected_state, list):
                    assert ti.state in expected_state
                else:
                    self.assertEqual(
                        ti.state,
                        expected_state,
                        f"Task {ti.task_id} has state {ti.state} instead of expected {expected_state}",
                    )

    def test_branch_follow_true(self):
        """ Take the true branch when the cron expressions matches"""

        branch_op = BranchOnCronOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            cron_expression="0 0 * * WED",
            use_task_execution_day=True,
            run_on_first_execution=False,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                "make_choice": State.SUCCESS,
                "branch_1": [
                    State.SUCCESS,
                    None,
                ],  # Results seem to be inconsistent - either option is fine
                "branch_2": State.SKIPPED,
            },
        )

    def test_branch_follow_false(self):
        """ Take the false branch when the cron expressions doesn't match"""
        branch_op = BranchOnCronOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            cron_expression="0 0 * * THU",
            use_task_execution_day=True,
            run_on_first_execution=False,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                "make_choice": State.SUCCESS,
                "branch_1": State.SKIPPED,
                "branch_2": [State.SUCCESS, None],
            },
        )

    def test_branch_follow_true_on_first_run(self):
        """ Take the true branch when run_on_first_execution is true, even if the cron expressions doesn't match """
        branch_op = BranchOnCronOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            cron_expression="0 0 * * THU",
            use_task_execution_day=True,
            run_on_first_execution=True,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                "make_choice": State.SUCCESS,
                "branch_1": [State.SUCCESS, None],
                "branch_2": State.SKIPPED,
            },
        )
