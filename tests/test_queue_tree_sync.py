from __future__ import annotations

import unittest

from queue_models import JobStatus, RenderJob
from queue_tree_sync import refresh_jobs_from_rop_metadata
from rop_metadata import RopInfo


class QueueTreeSyncTests(unittest.TestCase):
    def test_refresh_jobs_from_rop_metadata_preserves_overrides_by_default(self) -> None:
        job = RenderJob(
            hip_path=__file__,
            rop_path="/stage/CamTop",
            frame_range_mode="override",
            start_frame=250,
            end_frame=255,
            step=2,
        )
        job.runtime.status = JobStatus.QUEUED
        job.runtime.runtime_start_frame = 1
        job.runtime.runtime_end_frame = 2
        job.runtime.runtime_step = 1

        info = RopInfo(
            strict_frame_range=False,
            runtime_start_frame=300,
            runtime_end_frame=320,
            runtime_step=1,
            output_path="D:/renders/CamTop.$F4.exr",
        )
        restored: list[str] = []

        changed_ids = refresh_jobs_from_rop_metadata(
            [job],
            running_status=JobStatus.RUNNING,
            scan_rop_info_for_hip=lambda hip_path: {"/stage/CamTop": info},
            probe_rop_info=lambda hip_path, rop_path: None,
            mark_job_offline=lambda target, reason: None,
            restore_job_online_status=lambda target: restored.append(target.id),
            clear_job_resume_runtime_state=lambda target: None,
            normalize_output_display_path=lambda value: value,
            reset_override_to_rop=False,
        )

        self.assertEqual(changed_ids, [job.id])
        self.assertEqual(job.spec.frame_range_mode, "override")
        self.assertEqual(job.spec.start_frame, 250)
        self.assertEqual(job.spec.end_frame, 255)
        self.assertEqual(job.spec.step, 2)
        self.assertEqual(job.runtime.runtime_start_frame, 300)
        self.assertEqual(job.runtime.runtime_end_frame, 320)
        self.assertEqual(job.runtime.runtime_step, 1)
        self.assertEqual(job.view.out_file_sample_path, "D:/renders/CamTop.$F4.exr")
        self.assertEqual(restored, [job.id])

    def test_refresh_jobs_from_rop_metadata_can_reset_values_to_file_defaults(self) -> None:
        job = RenderJob(
            hip_path=__file__,
            rop_path="/stage/CamTop",
            frame_range_mode="override",
            start_frame=250,
            end_frame=255,
            step=2,
        )
        job.runtime.status = JobStatus.QUEUED
        cleared: list[str] = []

        info = RopInfo(
            strict_frame_range=False,
            runtime_start_frame=300,
            runtime_end_frame=320,
            runtime_step=1,
            output_path="D:/renders/CamTop.$F4.exr",
        )

        changed_ids = refresh_jobs_from_rop_metadata(
            [job],
            running_status=JobStatus.RUNNING,
            scan_rop_info_for_hip=lambda hip_path: {"/stage/CamTop": info},
            probe_rop_info=lambda hip_path, rop_path: None,
            mark_job_offline=lambda target, reason: None,
            restore_job_online_status=lambda target: None,
            clear_job_resume_runtime_state=lambda target: cleared.append(target.id),
            normalize_output_display_path=lambda value: value,
            reset_override_to_rop=True,
        )

        self.assertEqual(changed_ids, [job.id])
        self.assertEqual(job.spec.frame_range_mode, "use_rop")
        self.assertIsNone(job.spec.start_frame)
        self.assertIsNone(job.spec.end_frame)
        self.assertIsNone(job.spec.step)
        self.assertEqual(job.runtime.runtime_start_frame, 300)
        self.assertEqual(job.runtime.runtime_end_frame, 320)
        self.assertEqual(job.runtime.runtime_step, 1)
        self.assertEqual(cleared, [job.id])


if __name__ == "__main__":
    unittest.main()
