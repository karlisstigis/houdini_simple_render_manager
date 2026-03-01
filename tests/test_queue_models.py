from __future__ import annotations

import unittest

from queue_models import FrameHandlingMode, JobStatus, RenderJob


class QueueModelTests(unittest.TestCase):
    def test_render_job_flat_access_routes_to_nested_sections(self) -> None:
        job = RenderJob(
            hip_path="E:/shot/test.hip",
            rop_path="/out/mantra1",
            frame_range_mode="override",
            start_frame=1001,
            end_frame=1010,
            step=1,
            name="Test",
            status=JobStatus.QUEUED,
            enabled=True,
            frame_handling_mode=FrameHandlingMode.RENDER_MISSING,
        )
        self.assertEqual(job.spec.hip_path, "E:/shot/test.hip")
        self.assertEqual(job.runtime.status, JobStatus.QUEUED)
        job.error_summary = "boom"
        job.progress_text = "1004"
        job.enabled = False
        self.assertEqual(job.runtime.error_summary, "boom")
        self.assertEqual(job.view.progress_text, "1004")
        self.assertFalse(job.spec.enabled)


if __name__ == "__main__":
    unittest.main()
