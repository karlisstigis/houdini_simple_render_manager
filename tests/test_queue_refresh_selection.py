from __future__ import annotations

import unittest

from queue_refresh_selection import clamped_select_row, preserved_selection, target_selection


class QueueRefreshSelectionTests(unittest.TestCase):
    def test_preserved_selection(self) -> None:
        ids, single = preserved_selection(
            select_row=None,
            select_job_id=None,
            select_job_ids=None,
            current_selected_job_ids=["a", "b"],
            current_selected_job_id="c",
        )
        self.assertEqual(ids, ["a", "b"])
        self.assertIsNone(single)

        ids2, single2 = preserved_selection(
            select_row=None,
            select_job_id=None,
            select_job_ids=None,
            current_selected_job_ids=[],
            current_selected_job_id="c",
        )
        self.assertEqual(ids2, [])
        self.assertEqual(single2, "c")

    def test_target_selection(self) -> None:
        job_id, job_ids, job_set = target_selection(
            select_job_id=None,
            select_job_ids=None,
            preserved_job_id="x",
            preserved_job_ids=["a", "b"],
        )
        self.assertEqual(job_id, "x")
        self.assertEqual(job_ids, ["a", "b"])
        self.assertEqual(job_set, {"a", "b"})

    def test_clamped_select_row(self) -> None:
        self.assertIsNone(clamped_select_row(None, job_count=3))
        self.assertIsNone(clamped_select_row(1, job_count=0))
        self.assertEqual(clamped_select_row(-10, job_count=4), 0)
        self.assertEqual(clamped_select_row(99, job_count=4), 3)


if __name__ == "__main__":
    unittest.main()
