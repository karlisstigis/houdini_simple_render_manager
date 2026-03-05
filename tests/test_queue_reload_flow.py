from __future__ import annotations

import unittest

from queue_core.queue_models import JobStatus, RenderJob
from flows.queue_reload_flow import (
    RELOAD_ALL_EMPTY_MESSAGE,
    RELOAD_ALL_NOTIFICATION_LABEL,
    RELOAD_ALL_STATUS_TEXT,
    RELOAD_VALUES_NOTIFICATION_LABEL,
    RELOAD_VALUES_STATUS_TEXT,
    defer_reload_values_from_file,
    reloadable_jobs,
    run_reload_all_jobs_from_file,
)


class QueueReloadFlowTests(unittest.TestCase):
    def test_reloadable_jobs_filters_running(self) -> None:
        queued = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        running = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        running.runtime.status = JobStatus.RUNNING
        jobs = reloadable_jobs([queued, running], running_status=JobStatus.RUNNING)
        self.assertEqual(jobs, [queued])

    def test_defer_reload_values_from_file_uses_expected_payload(self) -> None:
        first = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        second = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        captured: list[dict] = []

        def _defer(target_jobs, **kwargs):  # type: ignore[no-untyped-def]
            captured.append({"target_jobs": list(target_jobs), **kwargs})

        defer_reload_values_from_file(
            [first, second],
            defer_reload_jobs_from_file=_defer,
        )

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["target_jobs"], [first, second])
        self.assertTrue(captured[0]["reset_override_to_rop"])
        self.assertEqual(captured[0]["status_text"], RELOAD_VALUES_STATUS_TEXT)
        self.assertEqual(captured[0]["notification_label"], RELOAD_VALUES_NOTIFICATION_LABEL)

    def test_run_reload_all_jobs_from_file_empty(self) -> None:
        running = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        running.runtime.status = JobStatus.RUNNING
        messages: list[tuple[str, int]] = []
        snapshot_calls: list[str] = []
        defer_calls: list[dict] = []

        started = run_reload_all_jobs_from_file(
            [running],
            running_status=JobStatus.RUNNING,
            write_queue_snapshot=lambda reason: snapshot_calls.append(reason) or True,
            defer_reload_jobs_from_file=lambda jobs, **kwargs: defer_calls.append(  # type: ignore[no-untyped-call]
                {"jobs": list(jobs), **kwargs}
            ),
            set_status_message=lambda message, timeout_ms: messages.append((message, timeout_ms)),
        )

        self.assertFalse(started)
        self.assertEqual(messages, [(RELOAD_ALL_EMPTY_MESSAGE, 3000)])
        self.assertEqual(snapshot_calls, [])
        self.assertEqual(defer_calls, [])

    def test_run_reload_all_jobs_from_file_dispatches_reload(self) -> None:
        queued = RenderJob("E:/shot/a.hip", "/stage/main", "use_rop")
        running = RenderJob("E:/shot/b.hip", "/stage/main", "use_rop")
        running.runtime.status = JobStatus.RUNNING
        snapshot_calls: list[str] = []
        defer_calls: list[dict] = []
        messages: list[tuple[str, int]] = []

        started = run_reload_all_jobs_from_file(
            [queued, running],
            running_status=JobStatus.RUNNING,
            write_queue_snapshot=lambda reason: snapshot_calls.append(reason) or True,
            defer_reload_jobs_from_file=lambda jobs, **kwargs: defer_calls.append(  # type: ignore[no-untyped-call]
                {"jobs": list(jobs), **kwargs}
            ),
            set_status_message=lambda message, timeout_ms: messages.append((message, timeout_ms)),
        )

        self.assertTrue(started)
        self.assertEqual(snapshot_calls, ["before_reload_all"])
        self.assertEqual(messages, [])
        self.assertEqual(len(defer_calls), 1)
        self.assertEqual(defer_calls[0]["jobs"], [queued])
        self.assertFalse(defer_calls[0]["reset_override_to_rop"])
        self.assertEqual(defer_calls[0]["status_text"], RELOAD_ALL_STATUS_TEXT)
        self.assertEqual(defer_calls[0]["notification_label"], RELOAD_ALL_NOTIFICATION_LABEL)


if __name__ == "__main__":
    unittest.main()
