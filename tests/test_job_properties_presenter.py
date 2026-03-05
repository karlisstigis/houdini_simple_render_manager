from __future__ import annotations

import unittest

from job_properties_presenter import (
    has_active_or_locked_jobs,
    selected_jobs_editable,
    selected_jobs_summary,
    should_show_custom_devices,
)
from queue_models import DeviceOverrideMode, RenderJob


class JobPropertiesPresenterTests(unittest.TestCase):
    def test_selected_jobs_summary(self) -> None:
        first = RenderJob("E:/a.hip", "/stage/CamA", "use_rop")
        second = RenderJob("E:/b.hip", "/stage/CamB", "use_rop")
        second.spec.reuse_retained_usd = True
        summary = selected_jobs_summary(
            [first, second],
            mixed_value=lambda values: (any(v != values[0] for v in values[1:]), values[0] if values else None),
            job_file_name=lambda job: job.spec.hip_path.split("/")[-1],
            job_rop_name=lambda job: job.spec.rop_path.split("/")[-1],
        )
        self.assertEqual(summary["selected_count"], 2)
        self.assertTrue(summary["mixed_file"])
        self.assertTrue(summary["mixed_rop"])
        self.assertTrue(summary["mixed_reuse"])

    def test_selected_jobs_editable(self) -> None:
        jobs = [RenderJob("E:/a.hip", "/stage/A", "use_rop"), RenderJob("E:/b.hip", "/stage/B", "use_rop")]
        self.assertTrue(selected_jobs_editable(jobs, can_edit_job=lambda _job: True))
        self.assertFalse(selected_jobs_editable(jobs, can_edit_job=lambda job: job is jobs[0]))

    def test_should_show_custom_devices(self) -> None:
        self.assertTrue(
            should_show_custom_devices(
                mixed_device_mode=False,
                first_device_mode=DeviceOverrideMode.SPECIFIC_GPUS.value,
            )
        )
        self.assertFalse(
            should_show_custom_devices(
                mixed_device_mode=True,
                first_device_mode=DeviceOverrideMode.SPECIFIC_GPUS.value,
            )
        )

    def test_has_active_or_locked_jobs(self) -> None:
        jobs = [RenderJob("E:/a.hip", "/stage/A", "use_rop"), RenderJob("E:/b.hip", "/stage/B", "use_rop")]
        self.assertTrue(
            has_active_or_locked_jobs(
                jobs,
                is_active_job=lambda job: job is jobs[1],
                is_locked_job=lambda _job: False,
            )
        )
        self.assertFalse(
            has_active_or_locked_jobs(
                jobs,
                is_active_job=lambda _job: False,
                is_locked_job=lambda _job: False,
            )
        )


if __name__ == "__main__":
    unittest.main()
