from __future__ import annotations

import unittest

from job_core.job_properties_actions import (
    device_mode_edit_spec,
    device_selection_edit_spec,
    retain_built_usd_edit_spec,
    reuse_retained_usd_edit_spec,
    single_process_render_edit_spec,
    usd_output_directory_custom_path_edit_spec,
    usd_output_directory_mode_edit_spec,
)
from queue_core.queue_models import DeviceOverrideMode, RenderJob, UsdOutputDirectoryMode


class JobPropertiesActionsTests(unittest.TestCase):
    def test_device_mode_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = device_mode_edit_spec(DeviceOverrideMode.SPECIFIC_GPUS.value)
        self.assertEqual(prop, "device_override_mode")
        self.assertTrue(apply_fn(job))
        self.assertIs(job.spec.device_override_mode, DeviceOverrideMode.SPECIFIC_GPUS)
        self.assertFalse(apply_fn(job))

    def test_device_selection_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = device_selection_edit_spec(" 2 , 0 ")
        self.assertEqual(prop, "device_selection")
        self.assertTrue(apply_fn(job))
        self.assertEqual(job.spec.device_selection, "2,0")
        self.assertFalse(apply_fn(job))

    def test_retain_built_usd_edit_spec_disables_reuse_when_unchecked(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = True
        prop, apply_fn = retain_built_usd_edit_spec(False)
        self.assertEqual(prop, "retain_built_usd")
        self.assertTrue(apply_fn(job))
        self.assertFalse(job.spec.retain_built_usd)
        self.assertFalse(job.spec.reuse_retained_usd)
        self.assertFalse(apply_fn(job))

    def test_single_process_render_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = single_process_render_edit_spec(True)
        self.assertEqual(prop, "render_all_frames_single_process")
        self.assertTrue(apply_fn(job))
        self.assertTrue(job.spec.render_all_frames_single_process)
        self.assertFalse(apply_fn(job))

    def test_reuse_retained_usd_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = reuse_retained_usd_edit_spec(True)
        self.assertEqual(prop, "reuse_retained_usd")
        self.assertTrue(apply_fn(job))
        self.assertTrue(job.spec.reuse_retained_usd)
        self.assertFalse(apply_fn(job))

    def test_usd_output_directory_mode_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = usd_output_directory_mode_edit_spec(UsdOutputDirectoryMode.PROJECT_PATH.value)
        self.assertEqual(prop, "usd_output_directory_mode")
        self.assertTrue(apply_fn(job))
        self.assertIs(job.spec.usd_output_directory_mode, UsdOutputDirectoryMode.PROJECT_PATH)
        self.assertFalse(apply_fn(job))

    def test_usd_output_directory_custom_path_edit_spec(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        prop, apply_fn = usd_output_directory_custom_path_edit_spec(" D:/cache/usd ")
        self.assertEqual(prop, "usd_output_directory_custom_path")
        self.assertTrue(apply_fn(job))
        self.assertEqual(job.spec.usd_output_directory_custom_path, "D:/cache/usd")
        self.assertFalse(apply_fn(job))


if __name__ == "__main__":
    unittest.main()
