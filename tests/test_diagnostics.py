from __future__ import annotations

import unittest

from diagnostics import DiagnosticsSnapshot, build_diagnostics_report


class DiagnosticsTests(unittest.TestCase):
    def test_build_diagnostics_report_includes_key_fields(self) -> None:
        report = build_diagnostics_report(
            DiagnosticsSnapshot(
                app_name="Houdini Simple Render Manager",
                queue_path="E:/queue.json",
                logs_dir="E:/logs",
                hbatch_path="C:/Houdini/bin/hbatch.exe",
                player_path="C:/djv.exe",
                queue_active=True,
                queue_paused=False,
                current_job_id="job-123",
                render_worker_active=True,
                scan_worker_active=False,
                render_worker_stderr="render error",
                scan_worker_stderr="",
                status_text="Running",
                recovery_headline="Recovered 1 interrupted job from the queue file.",
            )
        )
        self.assertIn("App: Houdini Simple Render Manager", report)
        self.assertIn("Queue File: E:/queue.json", report)
        self.assertIn("Render Worker Active: yes", report)
        self.assertIn("Last Recovery Summary: Recovered 1 interrupted job from the queue file.", report)
        self.assertIn("Render Worker Stderr:", report)


if __name__ == "__main__":
    unittest.main()
