from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_editing import mark_job_offline, restore_job_online_status
from queue_models import JobStatus, RenderJob
from queue_tree_sync import apply_hip_path_change, apply_rop_path_change, propagate_rop_path_change


class QueueTreeSyncTests(unittest.TestCase):
    def test_apply_hip_path_change_is_noop_for_same_value(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/karma1", frame_range_mode="use_rop", status=JobStatus.QUEUED)

        changed_jobs = apply_hip_path_change(
            [job],
            old_hip="E:/shot/test.hip",
            new_hip="E:/shot/test.hip",
            running_status=JobStatus.RUNNING,
        )

        self.assertEqual(changed_jobs, [])
        self.assertEqual(job.spec.hip_path, "E:/shot/test.hip")

    def test_apply_rop_path_change_updates_all_matching_jobs_before_sync(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            hip_path = str(Path(tmpdir) / "scene.hip")
            Path(hip_path).write_text("", encoding="utf-8")
            first = RenderJob(hip_path=hip_path, rop_path="/out/old", frame_range_mode="use_rop", status=JobStatus.QUEUED)
            second = RenderJob(hip_path=hip_path, rop_path="/out/old", frame_range_mode="use_rop", status=JobStatus.FAILED)
            running = RenderJob(hip_path=hip_path, rop_path="/out/old", frame_range_mode="use_rop", status=JobStatus.RUNNING)

            changed_jobs = apply_rop_path_change(
                [first, second, running],
                hip_path=hip_path,
                old_rop="/out/old",
                new_rop="/out/new",
                running_status=JobStatus.RUNNING,
            )

            self.assertEqual([job.id for job in changed_jobs], [first.id, second.id])
            self.assertEqual(first.spec.rop_path, "/out/new")
            self.assertEqual(second.spec.rop_path, "/out/new")
            self.assertEqual(running.spec.rop_path, "/out/old")

    def test_apply_rop_path_change_is_noop_for_same_value(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/karma1", frame_range_mode="use_rop", status=JobStatus.QUEUED)

        changed_jobs = apply_rop_path_change(
            [job],
            hip_path="E:/shot/test.hip",
            old_rop="/out/karma1",
            new_rop="/out/karma1",
            running_status=JobStatus.RUNNING,
        )

        self.assertEqual(changed_jobs, [])
        self.assertEqual(job.spec.rop_path, "/out/karma1")

    def test_propagate_rop_path_change_marks_job_offline_when_probe_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            hip_path = str(Path(tmpdir) / "scene.hip")
            Path(hip_path).write_text("", encoding="utf-8")
            job = RenderJob(hip_path=hip_path, rop_path="/out/old", frame_range_mode="use_rop", status=JobStatus.QUEUED)

            changed_ids = propagate_rop_path_change(
                [job],
                hip_path=hip_path,
                old_rop="/out/old",
                new_rop="/out/missing",
                running_status=JobStatus.RUNNING,
                probe_rop_info=lambda _hip, _rop: (_ for _ in ()).throw(RuntimeError("boom")),
                mark_job_offline=mark_job_offline,
                restore_job_online_status=restore_job_online_status,
                normalize_output_display_path=lambda value: value,
            )

            self.assertEqual(changed_ids, [job.id])
            self.assertEqual(job.spec.rop_path, "/out/missing")
            self.assertEqual(job.runtime.status, JobStatus.OFFLINE)
            self.assertIn("Failed to refresh ROP metadata", job.runtime.error_summary)


if __name__ == "__main__":
    unittest.main()
