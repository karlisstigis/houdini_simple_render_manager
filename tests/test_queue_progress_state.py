from __future__ import annotations

import unittest

from queue_models import JobStatus, RenderJob
from queue_progress_state import job_phase_display, parse_percent_value, queue_progress_split_values


class QueueProgressStateTests(unittest.TestCase):
    def test_job_phase_display_includes_chunk_info(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "use_rop")
        job.view.phase_text = "Render"
        job.runtime.chunk_total_runtime = 3
        job.runtime.chunk_index_runtime = 1
        job.runtime.chunk_attempt_runtime = 2
        self.assertEqual(job_phase_display(job), "Render (Chunk 2/3 r2)")

    def test_parse_percent_value(self) -> None:
        self.assertEqual(parse_percent_value("42%"), 42)
        self.assertEqual(parse_percent_value("200%"), 100)
        self.assertIsNone(parse_percent_value("n/a"))

    def test_queue_progress_split_done_with_single_process(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "use_rop")
        job.runtime.status = JobStatus.DONE
        job.runtime.allframesatonce_enabled = True
        build, render = queue_progress_split_values(job)
        self.assertEqual((build, render), (100, 100))

    def test_queue_progress_split_usd_build_phase(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "use_rop")
        job.runtime.allframesatonce_enabled = True
        job.view.phase_text = "USD Build"
        job.view.percent_text = "35%"
        build, render = queue_progress_split_values(job)
        self.assertEqual((build, render), (35, 0))

    def test_queue_progress_split_render_phase_sets_build_complete_when_pass_done(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "use_rop")
        job.runtime.allframesatonce_enabled = True
        job.view.phase_text = "Render"
        job.view.percent_text = "50%"
        job.view.build_pass_completed = True
        build, render = queue_progress_split_values(job)
        self.assertEqual((build, render), (100, 50))

    def test_queue_progress_split_default_uses_current_percent(self) -> None:
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "use_rop")
        job.view.percent_text = "66%"
        build, render = queue_progress_split_values(job)
        self.assertEqual((build, render), (None, 66))


if __name__ == "__main__":
    unittest.main()
