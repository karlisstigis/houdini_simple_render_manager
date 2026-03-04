from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_models import JobStatus, RenderJob
from queue_persistence import save_queue_payload
from queue_run_reporting import build_queue_run_summary, write_queue_snapshot


class QueueRunReportingTests(unittest.TestCase):
    def test_build_queue_run_summary_counts_and_average(self) -> None:
        done = RenderJob("E:/a.hip", "/out/a", "use_rop", status=JobStatus.DONE)
        done.view.render_frame_durations_sec = [1.0, 2.0]
        failed = RenderJob("E:/b.hip", "/out/b", "use_rop", status=JobStatus.FAILED)
        failed.view.render_frame_durations_sec = [3.0]
        summary = build_queue_run_summary([done, failed], {done.id, failed.id})
        self.assertIsNotNone(summary)
        assert summary is not None
        message, severity = summary
        self.assertEqual(severity, "warning")
        self.assertIn("2 job(s)", message)
        self.assertIn("Done 1", message)
        self.assertIn("Failed 1", message)
        self.assertIn("Avg frame 2.00s", message)

    def test_build_queue_run_summary_none_for_empty_started_set(self) -> None:
        job = RenderJob("E:/a.hip", "/out/a", "use_rop")
        self.assertIsNone(build_queue_run_summary([job], set()))

    def test_write_queue_snapshot_rolls_to_max_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            jobs = [RenderJob("E:/a.hip", "/out/a", "use_rop")]
            for _ in range(8):
                write_queue_snapshot(
                    base_dir=base,
                    reason="before_start",
                    jobs=jobs,
                    queue_view={},
                    active_job_id=None,
                    save_queue_payload_fn=save_queue_payload,
                    max_files=5,
                )
            backups = list((base / "queue_backups").glob("queue_*.json"))
            self.assertLessEqual(len(backups), 5)
            self.assertGreater(len(backups), 0)


if __name__ == "__main__":
    unittest.main()

