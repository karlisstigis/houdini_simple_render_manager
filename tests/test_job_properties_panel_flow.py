from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

from flows.job_properties_panel_flow import build_job_properties_state_for_selection
from queue_core.queue_models import DeviceOverrideMode, RenderJob


def _mixed_value(values: list[Any]) -> tuple[bool, Any]:
    if not values:
        return False, None
    first = values[0]
    return any(v != first for v in values[1:]), first


class JobPropertiesPanelFlowTests(unittest.TestCase):
    def test_empty_selection_uses_default_state(self) -> None:
        state = build_job_properties_state_for_selection(
            selected_jobs=[],
            panel_default_state=lambda: {"name_text": "-", "marker": "default"},
            mixed_value=_mixed_value,
            job_file_name=lambda job: job.spec.hip_path,
            job_rop_name=lambda job: job.spec.rop_path,
            single_job_retained_state=lambda _job: {},
            selected_retained_paths=lambda: [],
            can_edit_job_for_panel=lambda _job: True,
            device_option_states_for_jobs=lambda _jobs, _show, _editable: [],
            is_active_job=lambda _job: False,
            is_locked_job=lambda _job: False,
            unchecked_state=0,
            checked_state=2,
            partial_state=1,
            default_device_mode=DeviceOverrideMode.DEFAULT.value,
            default_usd_output_mode="default_temp",
        )
        self.assertEqual(state["marker"], "default")

    def test_single_selection_builds_full_state(self) -> None:
        job = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        job.spec.device_override_mode = DeviceOverrideMode.SPECIFIC_GPUS
        job.spec.device_selection = "0"
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True

        state = build_job_properties_state_for_selection(
            selected_jobs=[job],
            panel_default_state=lambda: {"marker": "default"},
            mixed_value=_mixed_value,
            job_file_name=lambda target: Path(target.spec.hip_path).name,
            job_rop_name=lambda target: Path(target.spec.rop_path).name,
            single_job_retained_state=lambda _job: {
                "retained_usd_path": "E:/cache/usd/job_a",
                "retained_usd_built_range": "250-260",
                "retained_usd_built_step": "1",
                "retained_usd_built_at": "2026-03-05 12:00:00",
                "retained_usd_status": "Reusable",
                "retained_usd_warning": "",
                "can_open": True,
            },
            selected_retained_paths=lambda: [],
            can_edit_job_for_panel=lambda _job: True,
            device_option_states_for_jobs=lambda _jobs, show, editable: [
                {"id": "0", "name": "GPU0", "enabled": editable and show}
            ],
            is_active_job=lambda _job: False,
            is_locked_job=lambda _job: False,
            unchecked_state=0,
            checked_state=2,
            partial_state=1,
            default_device_mode=DeviceOverrideMode.DEFAULT.value,
            default_usd_output_mode="default_temp",
        )
        self.assertEqual(state["device_mode"], DeviceOverrideMode.SPECIFIC_GPUS.value)
        self.assertEqual(state["retained_usd_status"], "Reusable")
        self.assertTrue(state["can_open"])
        self.assertTrue(state["can_delete"])

    def test_multi_selection_uses_multi_retained_and_blocks_delete_when_active(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        state = build_job_properties_state_for_selection(
            selected_jobs=[first, second],
            panel_default_state=lambda: {"marker": "default"},
            mixed_value=_mixed_value,
            job_file_name=lambda target: Path(target.spec.hip_path).name,
            job_rop_name=lambda target: Path(target.spec.rop_path).name,
            single_job_retained_state=lambda _job: {"can_open": False},
            selected_retained_paths=lambda: [
                Path("E:/cache/usd/a/__render__.usd"),
                Path("E:/cache/usd/b/__render__.usd"),
            ],
            can_edit_job_for_panel=lambda _job: True,
            device_option_states_for_jobs=lambda _jobs, _show, _editable: [],
            is_active_job=lambda job: job is second,
            is_locked_job=lambda _job: False,
            unchecked_state=0,
            checked_state=2,
            partial_state=1,
            default_device_mode=DeviceOverrideMode.DEFAULT.value,
            default_usd_output_mode="default_temp",
        )
        self.assertEqual(state["retained_usd_status"], "2 file(s) available")
        self.assertFalse(state["can_delete"])


if __name__ == "__main__":
    unittest.main()
