from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_models import FrameHandlingMode, JobStatus, RenderJob
from queue_persistence import (
    apply_job_order,
    apply_job_states,
    insert_jobs_from_entries,
    job_from_persisted_dict,
    job_to_persisted_dict,
    load_queue_payload,
    remove_jobs_by_ids,
    save_queue_payload,
)


class QueuePersistenceTests(unittest.TestCase):
    def test_job_round_trip_preserves_key_fields(self) -> None:
        job = RenderJob(
            hip_path="E:/shot/test.hip",
            rop_path="/out/mantra1",
            frame_range_mode="override",
            start_frame=1001,
            end_frame=1010,
            step=2,
            name="Test Job",
            status=JobStatus.FAILED,
            frame_handling_mode=FrameHandlingMode.OVERWRITE,
        )
        job.error_summary = "boom"
        job.runtime_start_frame = 1001
        job.runtime_end_frame = 1010
        job.runtime_step = 2
        cloned = job_from_persisted_dict(job_to_persisted_dict(job))
        self.assertIsNotNone(cloned)
        assert cloned is not None
        self.assertEqual(cloned.id, job.id)
        self.assertEqual(cloned.hip_path, job.hip_path)
        self.assertEqual(cloned.rop_path, job.rop_path)
        self.assertEqual(cloned.status, job.status)
        self.assertEqual(cloned.frame_handling_mode, job.frame_handling_mode)
        self.assertEqual(cloned.error_summary, "boom")

    def test_running_job_loads_back_as_canceled(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop", status=JobStatus.RUNNING)
        loaded = job_from_persisted_dict(job_to_persisted_dict(job))
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.status, JobStatus.INTERRUPTED)

    def test_active_job_id_loads_back_as_interrupted(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop", status=JobStatus.QUEUED)
        loaded = job_from_persisted_dict(job_to_persisted_dict(job), active_job_id=job.id)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.status, JobStatus.INTERRUPTED)
        self.assertTrue(loaded.interrupted_reason)
        self.assertIn("active", loaded.interrupted_reason.lower())

    def test_save_and_load_queue_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.json"
            jobs = [RenderJob(hip_path="E:/shot/a.hip", rop_path="/out/a", frame_range_mode="use_rop")]
            save_queue_payload(path, jobs=jobs, queue_view={"hidden_columns": [1]})
            payload = load_queue_payload(path)
            self.assertEqual(payload["version"], 1)
            self.assertEqual(payload["queue_view"]["hidden_columns"], [1])
            self.assertEqual(len(payload["jobs"]), 1)
            self.assertEqual(payload["active_job_id"], "")

    def test_save_queue_payload_persists_active_job_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.json"
            jobs = [RenderJob(hip_path="E:/shot/a.hip", rop_path="/out/a", frame_range_mode="use_rop")]
            save_queue_payload(path, jobs=jobs, queue_view={}, active_job_id=jobs[0].id)
            payload = load_queue_payload(path)
            self.assertEqual(payload["active_job_id"], jobs[0].id)

    def test_chunk_runtime_state_round_trip(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
        job.chunk_start_frame_runtime = 1001
        job.chunk_end_frame_runtime = 1010
        job.chunk_step_runtime = 1
        job.chunk_index_runtime = 1
        job.chunk_total_runtime = 4
        job.chunk_attempt_runtime = 2
        job.chunk_retry_count_runtime = 3
        job.chunk_ranges_runtime = [(1001, 1010, 1), (1011, 1020, 1)]
        job.chunk_retry_total_failures_runtime = 1
        loaded = job_from_persisted_dict(job_to_persisted_dict(job))
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.chunk_index_runtime, 1)
        self.assertEqual(loaded.chunk_total_runtime, 4)
        self.assertEqual(loaded.chunk_ranges_runtime, [(1001, 1010, 1), (1011, 1020, 1)])

    def test_interrupted_reason_includes_chunk_context(self) -> None:
        job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop", status=JobStatus.RUNNING)
        job.chunk_index_runtime = 1
        job.chunk_total_runtime = 3
        loaded = job_from_persisted_dict(job_to_persisted_dict(job))
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertIn("chunk 2/3", loaded.interrupted_reason.lower())

    def test_job_transform_helpers(self) -> None:
        a = RenderJob(hip_path="a.hip", rop_path="/out/a", frame_range_mode="use_rop")
        b = RenderJob(hip_path="b.hip", rop_path="/out/b", frame_range_mode="use_rop")
        c = RenderJob(hip_path="c.hip", rop_path="/out/c", frame_range_mode="use_rop")
        jobs = [a, b, c]
        reordered = apply_job_order(jobs, [c.id, a.id])
        self.assertEqual([job.id for job in reordered[:3]], [c.id, a.id, b.id])
        removed = remove_jobs_by_ids(reordered, [a.id])
        self.assertEqual([job.id for job in removed], [c.id, b.id])
        inserted = insert_jobs_from_entries(
            removed,
            [{"index": 1, "job": job_to_persisted_dict(a)}],
        )
        self.assertEqual([job.id for job in inserted], [c.id, a.id, b.id])
        a_updated = job_to_persisted_dict(a)
        a_updated["name"] = "Changed"
        updated = apply_job_states(inserted, [a_updated])
        self.assertEqual(updated[1].name, "Changed")


if __name__ == "__main__":
    unittest.main()
