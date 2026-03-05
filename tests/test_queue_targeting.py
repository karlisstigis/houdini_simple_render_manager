from __future__ import annotations

import unittest

from queue_core.queue_models import RenderJob
from queue_core.queue_targeting import (
    current_job_by_id,
    job_row_by_id,
    selected_job_for_row,
    selection_ids_for_refresh,
    tree_context_target_jobs,
)


class QueueTargetingTests(unittest.TestCase):
    def test_selection_ids_for_refresh(self) -> None:
        self.assertEqual(selection_ids_for_refresh(["a"], ["b"]), ["a"])
        self.assertEqual(selection_ids_for_refresh([], ["", "b"]), ["b"])
        self.assertIsNone(selection_ids_for_refresh([], []))
        self.assertIsNone(selection_ids_for_refresh([], None))

    def test_tree_context_target_jobs(self) -> None:
        j1 = RenderJob("E:/a.hip", "/stage/main", "use_rop")
        j2 = RenderJob("E:/a.hip", "/stage/alt", "use_rop")
        j3 = RenderJob("E:/b.hip", "/stage/main", "use_rop")
        jobs = [j1, j2, j3]

        hip_targets = tree_context_target_jobs(jobs, hip_path="E:/a.hip", rop_path="", kind="hip")
        self.assertEqual([j.id for j in hip_targets], [j1.id, j2.id])

        rop_targets = tree_context_target_jobs(jobs, hip_path="E:/a.hip", rop_path="/stage/alt", kind="rop")
        self.assertEqual([j.id for j in rop_targets], [j2.id])

        self.assertEqual(tree_context_target_jobs(jobs, hip_path="", rop_path="/stage/main", kind="rop"), [])

    def test_selected_job_for_row(self) -> None:
        j1 = RenderJob("E:/a.hip", "/stage/main", "use_rop")
        jobs = [j1]
        self.assertIs(selected_job_for_row(jobs, 0), j1)
        self.assertIsNone(selected_job_for_row(jobs, -1))
        self.assertIsNone(selected_job_for_row(jobs, 2))

    def test_job_row_by_id_and_current_job_by_id(self) -> None:
        j1 = RenderJob("E:/a.hip", "/stage/main", "use_rop")
        j2 = RenderJob("E:/b.hip", "/stage/main", "use_rop")
        jobs = [j1, j2]
        self.assertEqual(job_row_by_id(jobs, j2.id), 1)
        self.assertEqual(job_row_by_id(jobs, "missing"), -1)
        self.assertIs(current_job_by_id(jobs, j1.id), j1)
        self.assertIsNone(current_job_by_id(jobs, ""))


if __name__ == "__main__":
    unittest.main()
