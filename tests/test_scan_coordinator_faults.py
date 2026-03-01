from __future__ import annotations

import unittest
from pathlib import Path

from queue_models import RenderJob
from scan_coordinator import ScanCoordinator, ScanCoordinatorHooks


class ScanCoordinatorFaultTests(unittest.TestCase):
    def test_probe_and_apply_job_rop_metadata_returns_probe_failed_on_apply_exception(self) -> None:
        messages: list[tuple[str, str]] = []
        coordinator = ScanCoordinator(
            ScanCoordinatorHooks(
                current_hbatch_path=lambda: "hbatch.exe",
                project_houdini_scripts_dir=lambda: Path("."),
                hooks_dir_path=lambda: Path("."),
                hbatch_exists=lambda: True,
                scan_in_progress=lambda: False,
                send_scan_request=lambda _message_type, _payload: True,
                request_scan_sync=lambda _message_type, _payload, _timeout_ms: {
                    "type": "probe.result",
                    "request_id": "req1",
                    "payload": {
                        "rop_info": {
                            "error": None,
                            "strict_frame_range": False,
                            "runtime_start_frame": 1001,
                            "runtime_end_frame": 1010,
                            "runtime_step": 1,
                            "output_path": "X:/render/test.$F4.exr",
                            "returncode": 0,
                            "combined_output": "",
                        }
                    },
                },
                append_log=lambda source, text: messages.append((source, text)),
                safe_message=lambda _title, _text, _details=None: None,
                set_status_message=lambda _text, _timeout=None: None,
                normalize_output_display_path=lambda _value: (_ for _ in ()).throw(RuntimeError("normalize boom")),
                set_scan_hip_path_requested=lambda _hip_path: None,
            )
        )
        job = RenderJob("E:/shot/test.hip", "/out/karma1", "use_rop")

        result = coordinator.probe_and_apply_job_rop_metadata(job)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.startswith("probe_failed:"))
        self.assertTrue(any("Unexpected error" in text for _source, text in messages))


if __name__ == "__main__":
    unittest.main()
