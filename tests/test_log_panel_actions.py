from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from log_panel_actions import (
    delete_log_files,
    discover_log_files,
    log_deletion_feedback,
    selected_job_log_path,
)
from queue_core.queue_models import RenderJob


class LogPanelActionsTests(unittest.TestCase):
    def test_selected_job_log_path(self) -> None:
        self.assertIsNone(selected_job_log_path(None))
        job = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        self.assertIsNone(selected_job_log_path(job))
        job.runtime.log_file_path = " E:/logs/job.log "
        self.assertEqual(selected_job_log_path(job), Path("E:/logs/job.log"))

    def test_discover_log_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "a.log").write_text("a", encoding="utf-8")
            (root / "b.txt").write_text("b", encoding="utf-8")
            (root / "c.log").write_text("c", encoding="utf-8")
            files = discover_log_files(root)
            self.assertEqual([p.name for p in files], ["a.log", "c.log"])

    def test_delete_log_files_with_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            first = Path(tmpdir) / "ok.log"
            second = Path(tmpdir) / "fail.log"
            first.write_text("x", encoding="utf-8")
            second.write_text("y", encoding="utf-8")

            def _unlink(path: Path) -> None:
                if path.name == "fail.log":
                    raise OSError("denied")
                path.unlink()

            deleted, failed = delete_log_files([first, second], unlink_path=_unlink)
            self.assertEqual(deleted, 1)
            self.assertEqual(len(failed), 1)
            self.assertIn("fail.log", failed[0])

    def test_log_deletion_feedback(self) -> None:
        title, message, details = log_deletion_feedback(deleted=3, failed=[])
        self.assertEqual(title, "Logs")
        self.assertIn("Deleted 3 log file(s).", message)
        self.assertIsNone(details)

        title2, message2, details2 = log_deletion_feedback(
            deleted=1,
            failed=["a.log: denied", "b.log: denied"],
            max_failed_items=1,
        )
        self.assertEqual(title2, "Logs")
        self.assertIn("but 2 failed", message2)
        self.assertEqual(details2, "a.log: denied")


if __name__ == "__main__":
    unittest.main()
