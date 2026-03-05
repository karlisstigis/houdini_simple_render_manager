from __future__ import annotations

import unittest

from queue_models import JobStatus, RenderJob
from queue_path_sync_tasks import (
    enqueue_path_sync_task,
    run_next_path_sync_task,
    should_schedule_next_path_sync_task,
)


class QueuePathSyncTasksTests(unittest.TestCase):
    def test_enqueue_path_sync_task_copies_payload(self) -> None:
        pending: list[dict] = []
        payload = {"ids": ["a"]}
        enqueue_path_sync_task(pending, payload)
        self.assertEqual(pending, [{"ids": ["a"]}])
        payload["ids"].append("b")
        self.assertEqual(pending, [{"ids": ["a", "b"]}])
        self.assertIsNot(pending[0], payload)

    def test_should_schedule_next_path_sync_task(self) -> None:
        self.assertFalse(should_schedule_next_path_sync_task(path_sync_task_active=True, pending_tasks=[{"ids": ["x"]}]))
        self.assertFalse(should_schedule_next_path_sync_task(path_sync_task_active=False, pending_tasks=[]))
        self.assertTrue(should_schedule_next_path_sync_task(path_sync_task_active=False, pending_tasks=[{"ids": ["x"]}]))

    def test_run_next_path_sync_task_processes_jobs_and_notifies(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        second.runtime.status = JobStatus.OFFLINE
        pending = [
            {
                "ids": [first.id, second.id],
                "before_states": [{"id": first.id}, {"id": second.id}],
                "undo_select_job_ids": [first.id, second.id],
                "redo_select_job_ids": [second.id],
                "notification_label": "Reload All",
            }
        ]
        refreshed_tree: list[bool] = []
        refreshed_groups: list[list[str]] = []
        lock_releases: list[list[str]] = []
        history: list[dict] = []
        saved: list[bool] = []
        notifications: list[tuple[str, str]] = []

        refresh_needed = run_next_path_sync_task(
            jobs=[first, second],
            pending_tasks=pending,
            offline_status=JobStatus.OFFLINE,
            refresh_queue_tree_view=lambda: refreshed_tree.append(True),
            refresh_jobs_from_rop_metadata=lambda hip_jobs, reset_override_to_rop: refreshed_groups.append([job.id for job in hip_jobs]) or [job.id for job in hip_jobs],
            end_path_sync_lock=lambda ids: lock_releases.append(list(ids)),
            push_history_command=lambda payload: history.append(dict(payload)),
            job_states_for_ids=lambda ids: [{"id": job_id} for job_id in ids],
            save_queue_state=lambda: saved.append(True) or True,
            append_notification_message=lambda message, severity: notifications.append((message, severity)),
        )

        self.assertTrue(refresh_needed)
        self.assertEqual(pending, [])
        self.assertEqual(refreshed_tree, [True])
        self.assertEqual(sorted(sum(refreshed_groups, [])), sorted([first.id, second.id]))
        self.assertTrue(lock_releases)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["redo_select_job_ids"], [second.id])
        self.assertEqual(saved, [True])
        self.assertEqual(notifications, [("Reload All: 2 job(s) refreshed, 1 offline.", "warning")])

    def test_run_next_path_sync_task_releases_locks_when_refresh_fails(self) -> None:
        job = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        pending = [
            {
                "ids": [job.id],
                "before_states": [{"id": job.id}],
                "undo_select_job_ids": [job.id],
                "redo_select_job_ids": [job.id],
            }
        ]
        lock_releases: list[list[str]] = []

        with self.assertRaises(RuntimeError):
            run_next_path_sync_task(
                jobs=[job],
                pending_tasks=pending,
                offline_status=JobStatus.OFFLINE,
                refresh_queue_tree_view=lambda: None,
                refresh_jobs_from_rop_metadata=lambda _jobs, _reset: (_ for _ in ()).throw(RuntimeError("probe failed")),
                end_path_sync_lock=lambda ids: lock_releases.append(list(ids)),
                push_history_command=lambda _payload: None,
                job_states_for_ids=lambda _ids: [],
                save_queue_state=lambda: True,
                append_notification_message=lambda _message, _severity: None,
            )

        self.assertEqual(lock_releases, [[job.id]])


if __name__ == "__main__":
    unittest.main()
