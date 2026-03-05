from __future__ import annotations

import unittest

from queue_core.queue_models import RenderJob
from queue_core.queue_path_sync_lock import (
    advance_path_sync_overlay,
    begin_path_sync_lock,
    end_path_sync_lock,
    is_job_path_sync_locked,
    normalize_path_sync_job_ids,
)


class QueuePathSyncLockTests(unittest.TestCase):
    def test_normalize_path_sync_job_ids(self) -> None:
        normalized = normalize_path_sync_job_ids(["a", "", "  ", " b "])
        self.assertEqual(normalized, ["a", "b"])

    def test_is_job_path_sync_locked_accepts_job_or_id(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/main", "use_rop")
        counts = {job.id: 1}
        self.assertTrue(is_job_path_sync_locked(counts, job))
        self.assertTrue(is_job_path_sync_locked(counts, job.id))
        self.assertFalse(is_job_path_sync_locked(counts, "missing"))
        self.assertFalse(is_job_path_sync_locked(counts, None))

    def test_begin_path_sync_lock_increments_and_reports_transition(self) -> None:
        counts: dict[str, int] = {}
        ids, started_overlay = begin_path_sync_lock(counts, ["a", "a", "b"])
        self.assertEqual(ids, ["a", "a", "b"])
        self.assertTrue(started_overlay)
        self.assertEqual(counts, {"a": 2, "b": 1})

        ids_2, started_overlay_2 = begin_path_sync_lock(counts, ["a"])
        self.assertEqual(ids_2, ["a"])
        self.assertFalse(started_overlay_2)
        self.assertEqual(counts["a"], 3)

    def test_end_path_sync_lock_decrements_and_reports_transition(self) -> None:
        counts: dict[str, int] = {"a": 2, "b": 1}
        changed_ids, stopped_overlay = end_path_sync_lock(counts, ["a"])
        self.assertEqual(changed_ids, ["a"])
        self.assertFalse(stopped_overlay)
        self.assertEqual(counts, {"a": 1, "b": 1})

        changed_ids_2, stopped_overlay_2 = end_path_sync_lock(counts, ["a", "b"])
        self.assertEqual(changed_ids_2, ["a", "b"])
        self.assertTrue(stopped_overlay_2)
        self.assertEqual(counts, {})

    def test_advance_path_sync_overlay(self) -> None:
        next_progress, active = advance_path_sync_overlay({}, 0.4)
        self.assertFalse(active)
        self.assertEqual(next_progress, 0.0)

        next_progress_2, active_2 = advance_path_sync_overlay({"a": 1}, 0.98, step=0.05)
        self.assertTrue(active_2)
        self.assertAlmostEqual(next_progress_2, 0.03, places=6)


if __name__ == "__main__":
    unittest.main()
