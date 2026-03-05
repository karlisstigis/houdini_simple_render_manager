from __future__ import annotations

import unittest

from app_core.action_policy import (
    can_duplicate_jobs,
    can_edit_job,
    can_edit_job_column,
    can_open_queue_file,
    can_open_output_folder,
    can_preview_job,
    can_reload_jobs_from_file,
    can_remove_jobs,
    can_resume_job_from_output,
    can_scan_hip,
    can_start_queue,
    is_job_runnable,
    queue_row_status_label,
)
from queue_core.queue_models import JobStatus, RenderJob


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

    def test_can_edit_job_column_blocks_strict_range_columns(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        job.strict_frame_range = True
        decision = can_edit_job_column(job, column=3, is_active_job=False)
        self.assertFalse(decision.allowed)
        decision = can_edit_job_column(job, column=0, is_active_job=False)
        self.assertTrue(decision.allowed)

    def test_can_remove_jobs_blocks_only_active(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_remove_jobs([job], is_active_job_fn=lambda target: True)
        self.assertFalse(decision.allowed)

    def test_can_duplicate_jobs_blocks_only_active(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        decision = can_duplicate_jobs([job], is_active_job_fn=lambda target: True, scan_in_progress=False)
        self.assertFalse(decision.allowed)

    def test_can_open_queue_file_and_scan_hip(self) -> None:
        self.assertFalse(can_open_queue_file(queue_active=True, render_job_active=False, scan_in_progress=False).allowed)
        self.assertFalse(can_scan_hip(scan_in_progress=False, hbatch_exists=False).allowed)

    def test_more_action_policies(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop", status=JobStatus.CANCELED)
        self.assertFalse(
            can_reload_jobs_from_file(target_jobs=[job], is_active_job_fn=lambda _job: True, hbatch_exists=True).allowed
        )
        self.assertTrue(
            can_start_queue(
                queue_active=False,
                queue_paused=False,
                hbatch_exists=True,
                has_runnable=True,
                can_start_selected=False,
            ).allowed
        )
        self.assertFalse(
            can_resume_job_from_output(
                job,
                render_job_active=False,
                queue_active=False,
                hip_exists=False,
                hbatch_exists=True,
            ).allowed
        )
        self.assertFalse(can_preview_job(preview_path_exists=True, player_path_set=True, player_exists=False).allowed)
        self.assertFalse(can_open_output_folder(folder_exists=False).allowed)

    def test_queue_row_status_label(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        self.assertEqual(queue_row_status_label(job), JobStatus.QUEUED.value)
        job.enabled = False
        self.assertEqual(queue_row_status_label(job), "Disabled")


if __name__ == "__main__":
    unittest.main()
