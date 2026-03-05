from __future__ import annotations

import unittest

from queue_core.queue_models import JobStatus, RenderJob
from queue_core.queue_start_control import blocked_start_title, should_set_selected_rerun_status, start_queue_runnable_state


class QueueStartControlTests(unittest.TestCase):
    def test_start_queue_runnable_state(self) -> None:
        j1 = RenderJob("E:/a.hip", "/stage/a", "use_rop")
        j2 = RenderJob("E:/b.hip", "/stage/b", "use_rop")
        j2.runtime.status = JobStatus.DONE
        can_start_selected, has_runnable = start_queue_runnable_state(
            selected_job=j2,
            is_runnable=lambda job: bool(job and job.runtime.status == JobStatus.QUEUED),
            jobs=[j1, j2],
        )
        self.assertFalse(can_start_selected)
        self.assertTrue(has_runnable)

    def test_blocked_start_title(self) -> None:
        self.assertEqual(blocked_start_title("hbatch missing"), "hbatch Missing")
        self.assertEqual(blocked_start_title("Nothing runnable"), "Queue Empty")

    def test_should_set_selected_rerun_status(self) -> None:
        running = RenderJob("E:/a.hip", "/stage/a", "use_rop")
        running.runtime.status = JobStatus.RUNNING
        done = RenderJob("E:/a.hip", "/stage/a", "use_rop")
        done.runtime.status = JobStatus.DONE
        self.assertFalse(should_set_selected_rerun_status(None))
        self.assertFalse(should_set_selected_rerun_status(running))
        self.assertTrue(should_set_selected_rerun_status(done))


if __name__ == "__main__":
    unittest.main()
