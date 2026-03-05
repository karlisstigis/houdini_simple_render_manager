from __future__ import annotations

import unittest

from app_core.diagnostics_snapshot_builder import build_diagnostics_snapshot


class DiagnosticsSnapshotBuilderTests(unittest.TestCase):
    def test_build_diagnostics_snapshot_normalizes_values(self) -> None:
        snapshot = build_diagnostics_snapshot(
            app_name="Houdini Simple Render Manager",
            queue_path="E:/queue.json",
            logs_dir="E:/logs",
            hbatch_path="C:/H/bin/hbatch.exe",
            player_path="C:/Player/player.exe",
            queue_active=1,
            queue_paused=0,
            current_job_id="job-1",
            render_worker_active=True,
            scan_worker_active=False,
            render_worker_stderr=None,
            scan_worker_stderr="scan issue",
            status_text="Running",
            recovery_headline=None,
        )
        self.assertEqual(snapshot.queue_path, "E:/queue.json")
        self.assertTrue(snapshot.queue_active)
        self.assertFalse(snapshot.queue_paused)
        self.assertEqual(snapshot.render_worker_stderr, "")
        self.assertEqual(snapshot.scan_worker_stderr, "scan issue")
        self.assertEqual(snapshot.recovery_headline, "")


if __name__ == "__main__":
    unittest.main()
