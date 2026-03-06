from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from queue_core.queue_models import JobStatus, RenderJob
from queue_core.queue_state_coordinator import QueueStateCoordinator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_TEMP_ROOT = PROJECT_ROOT / ".tmp_test"


class _FakeWindow:
    def __init__(self, jobs: list[RenderJob]) -> None:
        self.jobs = jobs
        self.scheduled: list[tuple[object, int]] = []
        self._last_recovery_headline = ""

    def _startup_check_files_on_open(self) -> bool:
        return True

    def _startup_reload_all_jobs_on_open(self) -> bool:
        return False

    def _mark_job_offline(self, job: RenderJob, reason: str | None = None) -> None:
        if job.runtime.status != JobStatus.OFFLINE:
            job.runtime.offline_previous_status = job.runtime.status
            job.runtime.status = JobStatus.OFFLINE
        if reason:
            job.runtime.error_summary = reason

    def _restore_job_online_status(self, job: RenderJob) -> None:
        restore = job.runtime.offline_previous_status or JobStatus.QUEUED
        if restore == JobStatus.RUNNING:
            restore = JobStatus.QUEUED
        job.runtime.status = restore
        job.runtime.offline_previous_status = None

    def _schedule_deferred(self, callback, delay_ms: int) -> None:  # type: ignore[no-untyped-def]
        self.scheduled.append((callback, delay_ms))

    def _reload_all_jobs_from_files(self) -> None:
        pass

    def _refresh_queue_table(self, select_row=None) -> None:  # type: ignore[no-untyped-def]
        _ = select_row

    def _append_notification_message(self, message: str, severity: str) -> None:
        _ = (message, severity)

    def _append_log(self, source: str, text: str) -> None:
        _ = (source, text)

    def _set_status_message(self, message: str, timeout_ms: int = 0) -> None:
        _ = (message, timeout_ms)


class _FakeReloadWindow(_FakeWindow):
    def _startup_reload_all_jobs_on_open(self) -> bool:
        return True


class QueueStateCoordinatorTests(unittest.TestCase):
    def test_startup_file_checks_mark_missing_hip_offline(self) -> None:
        job = RenderJob(hip_path="E:/missing/test.hip", rop_path="/out/rop1", frame_range_mode="use_rop")
        window = _FakeWindow([job])

        changed = QueueStateCoordinator(window).apply_startup_job_file_checks()

        self.assertTrue(changed)
        self.assertEqual(job.runtime.status, JobStatus.OFFLINE)
        self.assertEqual(job.runtime.error_summary, "HIP file not found.")

    def test_startup_file_checks_restore_jobs_when_hip_returns(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        hip_path = TEST_TEMP_ROOT / f"startup_restore_{uuid4().hex}.hip"
        hip_path.write_text("hip", encoding="utf-8")
        self.addCleanup(hip_path.unlink, missing_ok=True)
        job = RenderJob(hip_path=str(hip_path), rop_path="/out/rop1", frame_range_mode="use_rop", status=JobStatus.OFFLINE)
        job.runtime.error_summary = "HIP file not found."
        job.runtime.offline_previous_status = JobStatus.DONE
        window = _FakeWindow([job])

        changed = QueueStateCoordinator(window).apply_startup_job_file_checks()

        self.assertTrue(changed)
        self.assertEqual(job.runtime.status, JobStatus.DONE)
        self.assertEqual(job.runtime.error_summary, "")
        self.assertIsNone(job.runtime.offline_previous_status)

    def test_schedule_startup_reload_all_uses_deferred_reload(self) -> None:
        job = RenderJob(hip_path="E:/missing/test.hip", rop_path="/out/rop1", frame_range_mode="use_rop")
        window = _FakeReloadWindow([job])

        scheduled = QueueStateCoordinator(window).schedule_startup_reload_all()

        self.assertTrue(scheduled)
        self.assertEqual(len(window.scheduled), 1)
        self.assertEqual(window.scheduled[0][0].__func__, window._reload_all_jobs_from_files.__func__)
        self.assertIs(window.scheduled[0][0].__self__, window)
        self.assertEqual(window.scheduled[0][1], 0)


if __name__ == "__main__":
    unittest.main()
