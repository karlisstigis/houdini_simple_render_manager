from __future__ import annotations

import unittest

from job_core.job_properties_state import (
    build_job_properties_panel_state,
    default_job_properties_panel_state,
    mixed_or_value_text,
    tri_state_bool,
)


class JobPropertiesStateTests(unittest.TestCase):
    def test_default_state_uses_passed_defaults(self) -> None:
        state = default_job_properties_panel_state(
            default_usd_output_mode="custom_path",
            default_usd_output_custom_path="D:/cache/usd",
            retained_usd_defaults={"retained_usd_status": "-", "can_open": False, "can_delete": False},
        )
        self.assertEqual(state["usd_output_directory_mode"], "custom_path")
        self.assertEqual(state["usd_output_directory_custom_path"], "D:/cache/usd")
        self.assertEqual(state["device_mode"], "default")
        self.assertEqual(state["single_process_render_check_state"], 0)

    def test_tri_state_bool(self) -> None:
        self.assertEqual(tri_state_bool(mixed=True, value=False, unchecked=0, checked=2, partial=1), 1)
        self.assertEqual(tri_state_bool(mixed=False, value=True, unchecked=0, checked=2, partial=1), 2)
        self.assertEqual(tri_state_bool(mixed=False, value=False, unchecked=0, checked=2, partial=1), 0)

    def test_build_panel_state_core_fields(self) -> None:
        state = build_job_properties_panel_state(
            mixed_name=False,
            first_name="JobA",
            mixed_file=True,
            first_file="scene.hip",
            mixed_rop=False,
            first_rop="CamTop",
            editable=True,
            mixed_device_mode=False,
            first_device_mode="specific_gpus",
            show_custom_devices=True,
            device_options=[{"id": "0", "name": "GPU0"}],
            mixed_single_process=False,
            first_single_process=True,
            mixed_retain=False,
            first_retain=True,
            mixed_reuse=True,
            first_reuse=False,
            mixed_usd_output_mode=False,
            first_usd_output_mode="project_path",
            mixed_usd_output_custom_path=True,
            first_usd_output_custom_path="D:/cache/usd",
            retained_usd_state={
                "retained_usd_path": "D:/cache/usd/job",
                "retained_usd_built_range": "1001-1010",
                "retained_usd_built_step": "1",
                "retained_usd_built_at": "2026-03-05 12:00:00",
                "retained_usd_status": "Reuse on next render",
                "retained_usd_warning": "",
                "can_open": True,
            },
            can_delete=True,
            unchecked_state=0,
            checked_state=2,
            partial_state=1,
            default_device_mode="default",
            default_usd_output_mode="default_temp",
        )
        self.assertEqual(state["name_text"], "JobA")
        self.assertEqual(state["file_text"], "Mixed")
        self.assertEqual(state["device_mode"], "specific_gpus")
        self.assertEqual(state["single_process_render_check_state"], 2)
        self.assertEqual(state["reuse_check_state"], 1)
        self.assertEqual(state["usd_output_directory_custom_path"], "")
        self.assertTrue(state["can_open"])
        self.assertTrue(state["can_delete"])

    def test_mixed_or_value_text(self) -> None:
        self.assertEqual(mixed_or_value_text(True, "abc"), "Mixed")
        self.assertEqual(mixed_or_value_text(False, ""), "-")


if __name__ == "__main__":
    unittest.main()
