from __future__ import annotations

import unittest

from action_policy import (
    can_duplicate_jobs,
    can_edit_job,
    can_open_queue_file,
    can_remove_jobs,
    can_retry_interrupted_jobs,
    can_scan_hip,
    is_job_runnable,
)
from queue_models import JobStatus, RenderJob


class ActionPolicyTests(unittest.TestCase):
    def test_is_job_runnable(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop", status=JobStatus.QUEUED)
        self.assertTrue(is_job_runnable(job))
        job.status = JobStatus.RUNNING
        self.assertFalse(is_job_runnable(job))

    def test_can_edit_job_blocks_active(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_edit_job(job, is_active_job=True)
        self.assertFalse(decision.allowed)

    def test_can_remove_jobs_blocks_only_active(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_remove_jobs([job], is_active_job_fn=lambda target: True)
        self.assertFalse(decision.allowed)

    def test_can_duplicate_jobs_blocks_only_active(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_duplicate_jobs([job], is_active_job_fn=lambda target: True, scan_in_progress=False)
        self.assertFalse(decision.allowed)

    def test_can_retry_interrupted_jobs_requires_interrupted(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_retry_interrupted_jobs([job], is_active_job_fn=lambda target: False)
        self.assertFalse(decision.allowed)
        job.status = JobStatus.INTERRUPTED
        decision = can_retry_interrupted_jobs([job], is_active_job_fn=lambda target: False)
        self.assertTrue(decision.allowed)

    def test_can_open_queue_file_and_scan_hip(self) -> None:
        self.assertFalse(can_open_queue_file(queue_active=True, render_job_active=False, scan_in_progress=False).allowed)
        self.assertFalse(can_scan_hip(scan_in_progress=False, hbatch_exists=False).allowed)


if __name__ == "__main__":
    unittest.main()
