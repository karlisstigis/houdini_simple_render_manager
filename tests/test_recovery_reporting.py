from __future__ import annotations

import unittest

from queue_models import JobStatus, RenderJob
from recovery_reporting import build_startup_recovery_summary


class RecoveryReportingTests(unittest.TestCase):
    def test_no_interrupted_jobs_returns_none(self) -> None:
        jobs = [RenderJob("a.hip", "/out/a", "use_rop", status=JobStatus.QUEUED)]
        self.assertIsNone(build_startup_recovery_summary(jobs))

    def test_generic_interrupted_job_builds_notice(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop", status=JobStatus.INTERRUPTED, name="Shot A")
        job.runtime.interrupted_reason = "App closed or crashed while this job was active."
        summary = build_startup_recovery_summary([job])
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.recovered_count, 1)
        self.assertIn("Recovered 1 interrupted job", summary.headline)
        self.assertIn("Recovered: Shot A.", summary.notices[0].message)
        self.assertIn("ended unexpectedly", summary.notices[0].message)

    def test_chunk_context_is_preserved_in_notice(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        job.runtime.status = JobStatus.INTERRUPTED
        job.runtime.interrupted_reason = "Render worker became unresponsive. Last active: chunk 3/8."
        summary = build_startup_recovery_summary([job])
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertIn("Last active chunk 3/8.", summary.notices[0].message)

    def test_missing_reason_uses_fallback(self) -> None:
        job = RenderJob("a.hip", "/out/a", "use_rop")
        job.runtime.status = JobStatus.INTERRUPTED
        summary = build_startup_recovery_summary([job])
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertIn("did not finish cleanly", summary.notices[0].message)

    def test_only_interrupted_jobs_are_included(self) -> None:
        interrupted = RenderJob("a.hip", "/out/a", "use_rop", status=JobStatus.INTERRUPTED)
        failed = RenderJob("b.hip", "/out/b", "use_rop", status=JobStatus.FAILED)
        done = RenderJob("c.hip", "/out/c", "use_rop", status=JobStatus.DONE)
        summary = build_startup_recovery_summary([interrupted, failed, done])
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.recovered_count, 1)


if __name__ == "__main__":
    unittest.main()
