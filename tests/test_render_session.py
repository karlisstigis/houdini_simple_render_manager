from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from PySide6 import QtCore

from queue_models import RenderJob
from render_session import RenderSessionController, RenderSessionHooks


class RenderSessionTests(unittest.TestCase):
    def _make_controller(self, tmpdir: str, started_payloads: list[dict]) -> RenderSessionController:
        logs: list[tuple[str, str]] = []

        def _append_log(stream: str, text: str) -> None:
            logs.append((stream, text))

        hooks = RenderSessionHooks(
            append_log=_append_log,
            write_job_log=lambda text: None,
            close_current_job_log=lambda: None,
            save_queue_state=lambda: True,
            refresh_job_row=lambda job_id: None,
            refresh_queue_table=lambda **kwargs: None,
            safe_message=lambda title, text, details=None: None,
            start_worker_render=lambda payload: started_payloads.append(payload) is None or True,
            ensure_husk_hook_files=lambda: {},
            build_render_preflight_script=lambda job, disable_husk_mplay, hook_paths: "print('ok')",
            current_hbatch_path=lambda: "C:/houdini/bin/hbatch.exe",
            normalize_output_display_path=lambda value: value,
            hscript_quote=lambda value: f'"{value}"',
            current_time=datetime.now,
            update_job_render_timing_stats=lambda job: None,
            update_phase_from_frame_sequence=lambda job, previous_frame_seen: None,
            update_job_phase_from_output=lambda job, text: None,
            cancel_phase_promote=lambda: None,
            mark_job_offline=lambda job, reason=None: None,
        )
        return RenderSessionController(
            hooks,
            hook_script_path_fn=lambda stem: Path(tmpdir) / f"{stem}.py",
            disable_husk_mplay_fn=lambda: False,
        )

    def test_build_render_worker_payload_contains_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="override",
                start_frame=1001,
                end_frame=1003,
                step=1,
            )
            payload = controller.build_render_worker_payload(job)
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertEqual(payload["job_id"], job.id)
            self.assertEqual(payload["commands"][-1], "quit")
            self.assertIn("render -V", payload["commands"][-2])

    def test_handle_worker_output_updates_progress_and_output_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="override",
                start_frame=1001,
                end_frame=1010,
                step=1,
            )
            controller.handle_worker_output(job, "frame 1004\n>>> Render E:/renders/img.1004.exr, driver\n")
            self.assertEqual(job.view.progress_text, "1004")
            self.assertTrue(job.view.percent_text.startswith("40%"))
            self.assertEqual(job.view.out_file_sample_path, "E:/renders/img.1004.exr")

    def test_handle_render_finished_can_continue_to_next_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
            job.runtime.chunk_ranges_runtime = [(1001, 1002, 1), (1003, 1004, 1)]
            job.runtime.chunk_total_runtime = 2
            job.runtime.chunk_index_runtime = 0
            job.runtime.chunk_attempt_runtime = 1
            job.runtime.chunk_start_frame_runtime = 1001
            job.runtime.chunk_end_frame_runtime = 1002
            job.runtime.chunk_step_runtime = 1

            result = controller.handle_render_finished(
                job,
                0,
                QtCore.QProcess.ExitStatus.NormalExit,
                was_canceled=False,
                advance_job_to_next_chunk=lambda target: setattr(target.runtime, "chunk_index_runtime", 1) or setattr(target.runtime, "chunk_start_frame_runtime", 1003) or setattr(target.runtime, "chunk_end_frame_runtime", 1004) or True,
                retry_delay_value=0,
            )
            self.assertTrue(result.continue_next_chunk)

    def test_finalize_worker_crash_marks_job_interrupted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
            job.runtime.chunk_total_runtime = 3
            job.runtime.chunk_index_runtime = 1
            controller.finalize_worker_crash(job, "Worker died")
            self.assertEqual(job.runtime.status.value, "Interrupted")
            self.assertEqual(job.runtime.exit_code, -1)
            self.assertIn("worker died", job.runtime.interrupted_reason.lower())
            self.assertIn("chunk 2/3", job.runtime.interrupted_reason.lower())


if __name__ == "__main__":
    unittest.main()
