from __future__ import annotations

import unittest

from flows.queue_start_flow import evaluate_job_start_preflight, start_queue_mode


class QueueStartFlowTests(unittest.TestCase):
    def test_start_queue_mode(self) -> None:
        self.assertEqual(
            start_queue_mode(queue_active=True, queue_paused=True, resume_existing=True, allowed=True),
            "resume_existing",
        )
        self.assertEqual(
            start_queue_mode(queue_active=True, queue_paused=False, resume_existing=False, allowed=True),
            "already_active",
        )
        self.assertEqual(
            start_queue_mode(queue_active=False, queue_paused=False, resume_existing=False, allowed=False),
            "blocked",
        )
        self.assertEqual(
            start_queue_mode(queue_active=False, queue_paused=False, resume_existing=False, allowed=True),
            "start_new",
        )

    def test_evaluate_job_start_preflight(self) -> None:
        missing_hbatch = evaluate_job_start_preflight(hbatch_exists=False, hip_exists=True)
        self.assertFalse(missing_hbatch.allowed)
        self.assertTrue(missing_hbatch.abort_queue)
        self.assertEqual(missing_hbatch.dialog_title, "hbatch Missing")

        missing_hip = evaluate_job_start_preflight(hbatch_exists=True, hip_exists=False)
        self.assertFalse(missing_hip.allowed)
        self.assertFalse(missing_hip.abort_queue)
        self.assertEqual(missing_hip.offline_reason, "HIP file not found.")

        ready = evaluate_job_start_preflight(hbatch_exists=True, hip_exists=True)
        self.assertTrue(ready.allowed)
        self.assertFalse(ready.abort_queue)
        self.assertIsNone(ready.offline_reason)


if __name__ == "__main__":
    unittest.main()
