from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_core.queue_models import RenderJob
from houdini_core.rop_metadata import RopInfo
from houdini_core.scan_coordinator import ScanCoordinator, ScanCoordinatorHooks


class ScanCoordinatorTests(unittest.TestCase):
    def _make_coordinator(self, responses: dict[str, dict] | None = None, *, hbatch_exists: bool = True) -> tuple[ScanCoordinator, list[tuple[str, str]], list[tuple[str, dict]]]:
        logs: list[tuple[str, str]] = []
        requests: list[tuple[str, dict]] = []
        responses = dict(responses or {})
        scripts_dir = Path(tempfile.gettempdir())
        hooks = ScanCoordinatorHooks(
            current_hbatch_path=lambda: "C:/houdini/bin/hbatch.exe",
            project_houdini_scripts_dir=lambda: scripts_dir,
            hooks_dir_path=lambda: scripts_dir,
            hbatch_exists=lambda: hbatch_exists,
            scan_in_progress=lambda: False,
            send_scan_request=lambda message_type, payload: requests.append((message_type, payload)) is None or True,
            request_scan_sync=lambda message_type, payload, timeout_ms: responses.get(message_type),
            append_log=lambda stream, text: logs.append((stream, text)),
            safe_message=lambda title, text, details=None: None,
            set_status_message=lambda text, timeout=None: None,
            normalize_output_display_path=lambda value: value,
            set_scan_hip_path_requested=lambda hip_path: None,
        )
        return ScanCoordinator(hooks), logs, requests

    def test_build_request_payload_includes_paths(self) -> None:
        coordinator, _logs, _requests = self._make_coordinator()
        payload = coordinator.build_request_payload(hip_path="E:/shot/test.hip", extra={"roots": ["/out"]})
        self.assertEqual(payload["hip_path"], "E:/shot/test.hip")
        self.assertEqual(payload["roots"], ["/out"])
        self.assertIn("scripts_dir", payload)

    def test_probe_rop_info_parses_response(self) -> None:
        coordinator, _logs, _requests = self._make_coordinator(
            {
                "scan.rop_info": {
                    "type": "probe.result",
                    "request_id": "a",
                    "payload": {
                        "rop_info": {
                            "error": None,
                            "strict_frame_range": True,
                            "all_frames_single_process": True,
                            "runtime_start_frame": 1001,
                            "runtime_end_frame": 1010,
                            "runtime_step": 1,
                            "output_path": "E:/renders/test.$F4.exr",
                            "returncode": 0,
                            "combined_output": "",
                        }
                    },
                }
            }
        )
        info = coordinator.probe_rop_info("E:/shot/test.hip", "/out/mantra1")
        self.assertIsInstance(info, RopInfo)
        assert info is not None
        self.assertEqual(info.runtime_start_frame, 1001)
        self.assertTrue(info.strict_frame_range)
        self.assertTrue(info.all_frames_single_process)

    def test_probe_and_apply_job_metadata_updates_job_without_overwriting_single_process_setting(self) -> None:
        coordinator, _logs, _requests = self._make_coordinator(
            {
                "scan.rop_info": {
                    "type": "probe.result",
                    "request_id": "a",
                    "payload": {
                        "rop_info": {
                            "error": None,
                            "strict_frame_range": False,
                            "all_frames_single_process": True,
                            "runtime_start_frame": 1001,
                            "runtime_end_frame": 1010,
                            "runtime_step": 1,
                            "output_path": "E:/renders/test.$F4.exr",
                            "returncode": 0,
                            "combined_output": "",
                        }
                    },
                }
            }
        )
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
        job.spec.render_all_frames_single_process = False
        coordinator.probe_and_apply_job_rop_metadata(job)
        self.assertEqual(job.runtime.runtime_start_frame, 1001)
        self.assertEqual(job.view.out_file_sample_path, "E:/renders/test.$F4.exr")
        self.assertFalse(job.spec.render_all_frames_single_process)

    def test_probe_and_apply_job_metadata_can_initialize_single_process_setting_when_requested(self) -> None:
        coordinator, _logs, _requests = self._make_coordinator(
            {
                "scan.rop_info": {
                    "type": "probe.result",
                    "request_id": "a",
                    "payload": {
                        "rop_info": {
                            "error": None,
                            "strict_frame_range": False,
                            "all_frames_single_process": True,
                            "runtime_start_frame": 1001,
                            "runtime_end_frame": 1010,
                            "runtime_step": 1,
                            "output_path": "E:/renders/test.$F4.exr",
                            "returncode": 0,
                            "combined_output": "",
                        }
                    },
                }
            }
        )
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
        coordinator.probe_and_apply_job_rop_metadata(job, apply_single_process_setting=True)
        self.assertTrue(job.spec.render_all_frames_single_process)


if __name__ == "__main__":
    unittest.main()
