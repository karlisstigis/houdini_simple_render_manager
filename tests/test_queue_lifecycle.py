from __future__ import annotations

import unittest

from queue_core.queue_lifecycle import (
    QueueLifecycleState,
    decide_next_job,
    evaluate_start_request,
    with_pause_toggled,
    with_queue_finished,
    with_queue_started,
    with_stop_requested,
)
from queue_core.queue_models import JobStatus, RenderJob


class QueueLifecycleTests(unittest.TestCase):
    def test_evaluate_start_request_requires_hbatch(self) -> None:
        state = QueueLifecycleState(
            queue_active=False,
            queue_paused=False,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
        )
        decision = evaluate_start_request(
            state,
            hbatch_exists=False,
            has_runnable=True,
            can_start_selected=False,
        )
        self.assertFalse(decision.allowed)
        self.assertIn("hbatch", decision.reason.lower())

    def test_with_queue_started_resets_runtime_flags(self) -> None:
        state = QueueLifecycleState(
            queue_active=False,
            queue_paused=True,
            stop_requested=True,
            canceling_current_job=True,
            current_job_id="abc",
            active_hbatch_pid=123,
            queue_rerun_statuses={JobStatus.FAILED},
            jobs_started_this_run={"x"},
            queue_next_search_index=9,
        )
        started = with_queue_started(state)
        self.assertTrue(started.queue_active)
        self.assertFalse(started.queue_paused)
        self.assertFalse(started.stop_requested)
        self.assertFalse(started.canceling_current_job)
        self.assertEqual(started.queue_next_search_index, 0)
        self.assertEqual(started.jobs_started_this_run, set())

    def test_with_pause_toggled_flips_pause_only(self) -> None:
        state = QueueLifecycleState(
            queue_active=True,
            queue_paused=False,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
        )
        toggled = with_pause_toggled(state)
        self.assertTrue(toggled.queue_paused)
        self.assertTrue(toggled.queue_active)

    def test_with_stop_requested_sets_canceling_when_render_active(self) -> None:
        state = QueueLifecycleState(
            queue_active=True,
            queue_paused=True,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
        )
        stopped = with_stop_requested(state, render_job_active=True)
        self.assertTrue(stopped.stop_requested)
        self.assertFalse(stopped.queue_paused)
        self.assertTrue(stopped.canceling_current_job)

    def test_with_queue_finished_returns_started_snapshot(self) -> None:
        state = QueueLifecycleState(
            queue_active=True,
            queue_paused=False,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id="abc",
            active_hbatch_pid=456,
            jobs_started_this_run={"j1", "j2"},
            queue_rerun_statuses={JobStatus.FAILED},
            queue_next_search_index=5,
        )
        finished, started = with_queue_finished(state)
        self.assertEqual(started, {"j1", "j2"})
        self.assertFalse(finished.queue_active)
        self.assertEqual(finished.jobs_started_this_run, set())
        self.assertEqual(finished.current_job_id, None)

    def test_decide_next_job_complete_when_no_runnable(self) -> None:
        state = QueueLifecycleState(
            queue_active=True,
            queue_paused=False,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
            jobs_started_this_run=set(),
            queue_next_search_index=0,
        )
        jobs = [RenderJob("E:/a.hip", "/out/a", "use_rop", status=JobStatus.DONE)]
        decision = decide_next_job(
            state,
            jobs=jobs,
            render_job_active=False,
            is_runnable=lambda job: bool(job and job.runtime.status == JobStatus.QUEUED),
        )
        self.assertIsNone(decision.job)
        self.assertEqual(decision.finish_message, "Queue complete")

    def test_decide_next_job_stopped_when_stop_requested(self) -> None:
        state = QueueLifecycleState(
            queue_active=True,
            queue_paused=False,
            stop_requested=True,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
        )
        decision = decide_next_job(
            state,
            jobs=[],
            render_job_active=False,
            is_runnable=lambda _job: False,
        )
        self.assertEqual(decision.finish_message, "Queue stopped")


if __name__ == "__main__":
    unittest.main()

