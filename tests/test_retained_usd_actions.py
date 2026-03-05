from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_core.queue_models import RenderJob
from usd_core.retained_usd_actions import (
    clear_deleted_retained_usd_runtime,
    delete_retained_usd_directories,
    first_retained_usd_folder,
)


class RetainedUsdActionsTests(unittest.TestCase):
    def test_first_retained_usd_folder(self) -> None:
        self.assertIsNone(first_retained_usd_folder([]))
        folder = first_retained_usd_folder([Path("E:/cache/job/__render__.usd")])
        self.assertEqual(folder, Path("E:/cache/job"))

    def test_delete_retained_usd_directories_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            first_dir = base / "usd_a"
            second_dir = base / "usd_b"
            first_dir.mkdir(parents=True, exist_ok=True)
            second_dir.mkdir(parents=True, exist_ok=True)
            first_file = first_dir / "__render__.usd"
            second_file = second_dir / "__render__.usd"
            first_file.write_text("a", encoding="utf-8")
            second_file.write_text("b", encoding="utf-8")

            result = delete_retained_usd_directories([first_file, second_file])

            self.assertTrue(result.deleted_any)
            self.assertIsNone(result.error)
            self.assertFalse(first_dir.exists())
            self.assertFalse(second_dir.exists())

    def test_clear_deleted_retained_usd_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            deleted_dir = (base / "deleted").resolve()
            kept_dir = (base / "kept").resolve()
            deleted_dir.mkdir(parents=True, exist_ok=True)
            kept_dir.mkdir(parents=True, exist_ok=True)
            deleted_file = deleted_dir / "__render__.usd"
            kept_file = kept_dir / "__render__.usd"
            deleted_file.write_text("x", encoding="utf-8")
            kept_file.write_text("y", encoding="utf-8")

            deleted_job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
            kept_job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
            deleted_job.runtime.retained_usd_path = str(deleted_file)
            kept_job.runtime.retained_usd_path = str(kept_file)

            cleared_ids: list[str] = []

            def _clear_runtime(job: RenderJob) -> None:
                cleared_ids.append(job.id)
                job.runtime.retained_usd_path = ""

            changed_ids = clear_deleted_retained_usd_runtime(
                [deleted_job, kept_job],
                {deleted_dir},
                clear_runtime=_clear_runtime,
            )

            self.assertEqual(changed_ids, [deleted_job.id])
            self.assertEqual(cleared_ids, [deleted_job.id])
            self.assertEqual(deleted_job.runtime.retained_usd_path, "")
            self.assertEqual(kept_job.runtime.retained_usd_path, str(kept_file))


if __name__ == "__main__":
    unittest.main()
