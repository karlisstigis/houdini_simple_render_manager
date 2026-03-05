from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from PySide6 import QtCore

from queue_core.queue_models import DeviceOverrideMode, RenderJob
from render_session import RenderSessionController, RenderSessionHooks


class RenderSessionTests(unittest.TestCase):
    def _make_controller(self, tmpdir: str, started_payloads: list[dict]) -> RenderSessionController:
        logs: list[tuple[str, str]] = []
        synced_jobs: list[str] = []

        def _append_log(stream: str, text: str) -> None:
            logs.append((stream, text))

        def _build_render_environment(job: RenderJob) -> dict[str, str]:
            env = {"HSRM_DEVICE_MODE": DeviceOverrideMode.DEFAULT.value}
            if job.spec.retain_built_usd:
                env["HSRM_RETAIN_USD_ENABLED"] = "1"
                env["HSRM_RETAIN_USD_OUTPUT_PATH"] = "E:/cache/test.usd"
                env["HSRM_REUSE_EXISTING_USD"] = "0"
            return env

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
            build_render_environment=_build_render_environment,
            normalize_output_display_path=lambda value: value,
            hscript_quote=lambda value: f'"{value}"',
            current_time=datetime.now,
            update_job_render_timing_stats=lambda job: None,
            update_phase_from_frame_sequence=lambda job, previous_frame_seen: None,
            update_job_phase_from_output=lambda job, text: None,
            cancel_phase_promote=lambda: None,
            mark_job_offline=lambda job, reason=None: None,
            sync_retained_usd_file_state=lambda job: synced_jobs.append(job.id),
        )
        controller = RenderSessionController(
            hooks,
            hook_script_path_fn=lambda stem: Path(tmpdir) / f"{stem}.py",
            disable_husk_mplay_fn=lambda: False,
        )
        controller._test_synced_jobs = synced_jobs  # type: ignore[attr-defined]
        return controller

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
            self.assertEqual(payload["environment"]["HSRM_DEVICE_MODE"], DeviceOverrideMode.DEFAULT.value)

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

    def test_handle_worker_output_syncs_retained_usd_when_metadata_write_is_pending(self) -> None:
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
                retain_built_usd=True,
            )
            job.runtime.retained_usd_metadata_pending_write = True

            controller.handle_worker_output(job, "frame 1004\n")

            self.assertIn(job.id, controller._test_synced_jobs)  # type: ignore[attr-defined]

    def test_handle_worker_output_updates_retained_usd_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="use_rop",
            )
            controller.handle_worker_output(job, "__HSRM_RETAIN_USD__|existing||E:/cache/scene.usd\n")
            self.assertEqual(job.runtime.retained_usd_path, "E:/cache/scene.usd")
            self.assertTrue(job.runtime.retained_usd_exists)

    def test_handle_worker_output_ignores_relative_retained_usd_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="use_rop",
            )
            controller.handle_worker_output(job, "__HSRM_RETAIN_USD__|planned||__render__.usd\n")
            self.assertEqual(job.runtime.retained_usd_path, "")
            self.assertFalse(job.runtime.retained_usd_verified)

    def test_handle_worker_output_captures_preflight_resolved_retained_usd_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="use_rop",
            )
            controller.handle_worker_output(job, "[Preflight][RetainUSD] Resolved Output File -> E:/cache/scene.usd (lopoutput)\n")
            self.assertEqual(job.runtime.retained_usd_path, "E:/cache/scene.usd")
            self.assertTrue(job.runtime.retained_usd_verified)
            self.assertFalse(job.runtime.retained_usd_exists)

    def test_handle_worker_output_ignores_relative_preflight_retained_usd_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="use_rop",
            )
            controller.handle_worker_output(job, "[Preflight][RetainUSD] Resolved Output File -> __render__.usd (lopoutput)\n")
            self.assertEqual(job.runtime.retained_usd_path, "")
            self.assertFalse(job.runtime.retained_usd_verified)

    def test_build_render_worker_payload_includes_retain_usd_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(
                hip_path="E:/shot/test.hip",
                rop_path="/out/mantra1",
                frame_range_mode="use_rop",
                retain_built_usd=True,
            )
            payload = controller.build_render_worker_payload(job)
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertIn("HSRM_RETAIN_USD_ENABLED", payload["environment"])
            self.assertEqual(payload["environment"]["HSRM_RETAIN_USD_ENABLED"], "1")

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

    def test_handle_render_finished_canceled_does_not_sync_retained_usd_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
            controller.handle_render_finished(
                job,
                1,
                QtCore.QProcess.ExitStatus.NormalExit,
                was_canceled=True,
                advance_job_to_next_chunk=lambda target: False,
                retry_delay_value=0,
            )
            self.assertNotIn(job.id, controller._test_synced_jobs)  # type: ignore[attr-defined]

    def test_handle_render_finished_success_syncs_retained_usd_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            started_payloads: list[dict] = []
            controller = self._make_controller(tmpdir, started_payloads)
            job = RenderJob(hip_path="E:/shot/test.hip", rop_path="/out/mantra1", frame_range_mode="use_rop")
            controller.handle_render_finished(
                job,
                0,
                QtCore.QProcess.ExitStatus.NormalExit,
                was_canceled=False,
                advance_job_to_next_chunk=lambda target: False,
                retry_delay_value=0,
            )
            self.assertIn(job.id, controller._test_synced_jobs)  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
