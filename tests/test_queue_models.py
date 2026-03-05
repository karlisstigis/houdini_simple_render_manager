from __future__ import annotations

import unittest

from queue_core.queue_models import DeviceOverrideMode, FrameHandlingMode, JobStatus, RenderJob, UsdOutputDirectoryMode


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

    def test_render_job_display_helpers_use_nested_sections(self) -> None:
        job = RenderJob(
            hip_path="E:/shot/test.hip",
            rop_path="/out/mantra1",
            frame_range_mode="override",
            start_frame=1001,
            end_frame=1010,
            step=2,
            name="",
            status=JobStatus.QUEUED,
            enabled=True,
            frame_handling_mode=FrameHandlingMode.OVERWRITE,
        )
        self.assertEqual(job.display_name(), "test | mantra1")
        self.assertEqual(job.frame_display(), "1001-1010x2")
        self.assertEqual(job.frame_range_display(), "1001-1010")
        self.assertEqual(job.step_display(), "2")
        self.assertEqual(job.total_override_frames(), 5)
        self.assertEqual(job.frame_handling_label(), "Overwrite")

    def test_render_job_display_helpers_use_runtime_range_when_not_overridden(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/out/mantra1", "use_rop")
        job.runtime.runtime_start_frame = 1001.0
        job.runtime.runtime_end_frame = 1003.0
        job.runtime.runtime_step = 1.0
        self.assertEqual(job.frame_range_display(), "1001-1003")
        self.assertEqual(job.step_display(), "1")

    def test_device_summary_uses_defaults_and_overrides(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/out/mantra1", "use_rop")
        self.assertEqual(job.device_summary(DeviceOverrideMode.ALL_GPUS), "Default (All GPUs)")
        job.spec.device_override_mode = DeviceOverrideMode.SPECIFIC_GPUS
        job.spec.device_selection = "0,1"
        self.assertEqual(job.device_summary(DeviceOverrideMode.ALL_GPUS), "0,1")

    def test_usd_output_directory_mode_defaults_to_temp(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/out/mantra1", "use_rop")
        self.assertFalse(job.spec.render_all_frames_single_process)
        self.assertEqual(job.spec.usd_output_directory_mode, UsdOutputDirectoryMode.DEFAULT_TEMP)
        self.assertEqual(job.spec.usd_output_directory_custom_path, "")


if __name__ == "__main__":
    unittest.main()
