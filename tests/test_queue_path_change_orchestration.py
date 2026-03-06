from __future__ import annotations

import unittest

from queue_core.queue_models import JobStatus, RenderJob
from queue_core.queue_path_change_orchestration import (
    affected_job_ids_for_hip_path_change,
    affected_job_ids_for_rop_path_change,
    apply_hip_path_change_immediately,
    defer_finalize_path_change,
    defer_reload_jobs_from_file,
)


class QueuePathChangeOrchestrationTests(unittest.TestCase):
    def test_affected_job_ids_for_hip_path_change(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        third = RenderJob("E:/shot/a.hip", "/stage/alt", "use_rop")
        ids = affected_job_ids_for_hip_path_change([first, second, third], "E:/shot/a.hip")
        self.assertEqual(ids, [first.id, third.id])

    def test_affected_job_ids_for_rop_path_change(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/a.hip", "/stage/alt", "use_rop")
        third = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        ids = affected_job_ids_for_rop_path_change([first, second, third], "E:/shot/a.hip", "/stage/main")
        self.assertEqual(ids, [first.id])

    def test_apply_hip_path_change_immediately_skips_running_jobs(self) -> None:
        queued = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        running = RenderJob("E:/shot/a.hip", "/stage/alt", "use_rop")
        running.runtime.status = JobStatus.RUNNING
        changed_ids = apply_hip_path_change_immediately(
            [queued, running],
            old_hip="E:/shot/a.hip",
            new_hip="E:/shot/new.hip",
            running_status=JobStatus.RUNNING,
        )
        self.assertEqual(changed_ids, [queued.id])
        self.assertEqual(queued.spec.hip_path, "E:/shot/new.hip")
        self.assertEqual(running.spec.hip_path, "E:/shot/a.hip")

    def test_defer_finalize_path_change_enqueues_task_and_locks(self) -> None:
        locked: list[list[str]] = []
        queued_tasks: list[dict] = []

        defer_finalize_path_change(
            changed_ids=["job-a", "job-b"],
            before_states=[{"id": "job-a"}],
            undo_select_job_ids=["job-a", "job-b"],
            redo_select_job_ids=["job-b"],
            status_text="Updating path...",
            begin_path_sync_lock=lambda ids: locked.append(list(ids)),
            enqueue_path_sync_task=lambda task: queued_tasks.append(dict(task)),
        )

        self.assertEqual(locked, [["job-a", "job-b"]])
        self.assertEqual(len(queued_tasks), 1)
        self.assertEqual(queued_tasks[0]["ids"], ["job-a", "job-b"])
        self.assertEqual(queued_tasks[0]["status_text"], "Updating path...")

    def test_defer_reload_jobs_from_file_preserves_selection_ids(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        locked: list[list[str]] = []
        queued_tasks: list[dict] = []

        defer_reload_jobs_from_file(
            [first, second],
            reset_override_to_rop=True,
            status_text="Reloading...",
            notification_label="Reload Values from File",
            preserved_selection_job_ids=[second.id],
            job_states_for_ids=lambda ids: [{"id": job_id} for job_id in ids],
            begin_path_sync_lock=lambda ids: locked.append(list(ids)),
            enqueue_path_sync_task=lambda task: queued_tasks.append(dict(task)),
        )

        self.assertEqual(locked, [[first.id, second.id]])
        self.assertEqual(len(queued_tasks), 1)
        task = queued_tasks[0]
        self.assertTrue(task["reset_override_to_rop"])
        self.assertEqual(task["notification_label"], "Reload Values from File")
        self.assertEqual(task["undo_select_job_ids"], [second.id])
        self.assertEqual(task["redo_select_job_ids"], [second.id])
        self.assertEqual(task["before_states"], [{"id": first.id}, {"id": second.id}])


if __name__ == "__main__":
    unittest.main()
