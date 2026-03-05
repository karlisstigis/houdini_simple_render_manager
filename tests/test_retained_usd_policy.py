from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from houdini_simple_render_manager import MainWindow
from queue_core.queue_models import DeviceOverrideMode, JobStatus, RenderJob, UsdOutputDirectoryMode


class RetainedUsdPolicyTests(unittest.TestCase):
    def _make_window(self) -> MainWindow:
        return MainWindow.__new__(MainWindow)

    def test_update_job_properties_panel_uses_default_usd_output_mode_when_no_jobs_selected(self) -> None:
        window = self._make_window()

        class _Config:
            def get(self, key: str, default=None):
                values = {
                    "default_usd_output_directory_mode": UsdOutputDirectoryMode.CUSTOM_PATH.value,
                    "default_usd_output_directory_custom_path": "D:/cache/usd",
                }
                return values.get(key, default)

        class _Panel:
            def __init__(self) -> None:
                self.state = None

            def set_state(self, state: dict[str, object]) -> None:
                self.state = state

        panel = _Panel()
        window.config = _Config()  # type: ignore[attr-defined]
        window.job_properties_panel = panel  # type: ignore[attr-defined]
        window._selected_jobs = lambda: []  # type: ignore[attr-defined]

        window._update_job_properties_panel()

        assert panel.state is not None
        self.assertEqual(panel.state["usd_output_directory_mode"], UsdOutputDirectoryMode.CUSTOM_PATH.value)
        self.assertEqual(panel.state["usd_output_directory_custom_path"], "D:/cache/usd")

    def test_single_job_retained_usd_panel_state_syncs_once(self) -> None:
        window = self._make_window()
        sync_calls: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"
        job.runtime.retained_usd_exists = True

        window._sync_retained_usd_file_state = lambda target: sync_calls.append(target.id)  # type: ignore[attr-defined]
        window._load_retained_usd_metadata = lambda path: {"start_frame": 1001, "end_frame": 1010, "step": 1, "built_at": "2026-03-03T12:34:56"}  # type: ignore[attr-defined]
        window._retained_usd_stale_reason = lambda target: ""  # type: ignore[attr-defined]
        window._retained_usd_hip_stale_reason = lambda target, metadata: ""  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]
        window._retained_usd_status_text = lambda target, metadata: "Reuse on next render"  # type: ignore[attr-defined]
        window._configured_retained_usd_folder_preview = lambda target: "E:/preview"  # type: ignore[attr-defined]

        state = window._single_job_retained_usd_panel_state(job)

        self.assertEqual(sync_calls, [job.id])
        self.assertEqual(state["retained_usd_path"], str(Path("E:/cache/scene.usd").parent))
        self.assertEqual(state["retained_usd_built_range"], "1001-1010")
        self.assertEqual(state["retained_usd_built_step"], "1")
        self.assertEqual(state["retained_usd_built_at"], "2026-03-03 12:34:56")
        self.assertEqual(state["retained_usd_status"], "Reuse on next render")
        self.assertTrue(state["can_open"])

    def test_build_info_formats_range_and_step(self) -> None:
        self.assertEqual(
            MainWindow._retained_usd_build_info({"start_frame": 1001, "end_frame": 1010, "step": 1}),
            ("1001-1010", "1"),
        )
        self.assertEqual(MainWindow._retained_usd_build_info(None), ("-", "-"))

    def test_built_at_text_formats_iso_timestamp(self) -> None:
        self.assertEqual(
            MainWindow._retained_usd_built_at_text({"built_at": "2026-03-03T12:34:56"}),
            "2026-03-03 12:34:56",
        )
        self.assertEqual(MainWindow._retained_usd_built_at_text(None), "-")

    def test_reuse_allowed_when_range_fits_and_built_step_is_one(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1005, end_frame=1015, step=3)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._load_retained_usd_metadata = lambda path: {"start_frame": 1001, "end_frame": 1020, "step": 1}  # type: ignore[attr-defined]
        window._current_retained_usd_reuse_range = lambda target: (1005, 1015, 3)  # type: ignore[attr-defined]

        self.assertEqual(window._retained_usd_invalid_reason(job), "")

    def test_reuse_rejected_when_requested_range_exceeds_built_range(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=995, end_frame=1015, step=1)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._load_retained_usd_metadata = lambda path: {"start_frame": 1001, "end_frame": 1020, "step": 1}  # type: ignore[attr-defined]
        window._current_retained_usd_reuse_range = lambda target: (995, 1015, 1)  # type: ignore[attr-defined]

        self.assertEqual(
            window._retained_usd_invalid_reason(job),
            "Cannot reuse USD: current frame range exceeds the built USD range.",
        )

    def test_reuse_allowed_for_non_one_built_step_when_requested_step_matches_and_alignment_matches(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1002, end_frame=1010, step=2)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._load_retained_usd_metadata = lambda path: {"start_frame": 1000, "end_frame": 1020, "step": 2}  # type: ignore[attr-defined]
        window._current_retained_usd_reuse_range = lambda target: (1002, 1010, 2)  # type: ignore[attr-defined]

        self.assertEqual(window._retained_usd_invalid_reason(job), "")

    def test_reuse_rejected_for_non_one_built_step_when_alignment_does_not_match(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1001, end_frame=1009, step=2)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._load_retained_usd_metadata = lambda path: {"start_frame": 1000, "end_frame": 1020, "step": 2}  # type: ignore[attr-defined]
        window._current_retained_usd_reuse_range = lambda target: (1001, 1009, 2)  # type: ignore[attr-defined]

        self.assertEqual(
            window._retained_usd_invalid_reason(job),
            "Cannot reuse USD: current frame range is not aligned with the built USD step.",
        )

    def test_reuse_rejected_for_different_requested_step_when_built_step_is_not_one(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1002, end_frame=1010, step=5)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._load_retained_usd_metadata = lambda path: {"start_frame": 1000, "end_frame": 1020, "step": 2}  # type: ignore[attr-defined]
        window._current_retained_usd_reuse_range = lambda target: (1002, 1010, 5)  # type: ignore[attr-defined]

        self.assertEqual(
            window._retained_usd_invalid_reason(job),
            "Cannot reuse USD: retained USD must be built with step 1 to reuse with a different step.",
        )

    def test_status_text_reports_reuse_for_valid_existing_usd(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1002, end_frame=1010, step=2)
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"
        job.runtime.retained_usd_exists = True
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]

        self.assertEqual(window._retained_usd_status_text(job, {"start_frame": 1000, "end_frame": 1020, "step": 2}), "Reuse on next render")

    def test_status_text_reports_rebuild_when_reuse_is_not_possible(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=1002, end_frame=1010, step=2)
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"
        job.runtime.retained_usd_exists = True
        window._retained_usd_invalid_reason = lambda target: "Cannot reuse USD: current frame range exceeds the built USD range."  # type: ignore[attr-defined]

        self.assertEqual(window._retained_usd_status_text(job, {"start_frame": 1000, "end_frame": 1004, "step": 2}), "Build on next render")

    def test_status_text_reports_build_when_retain_and_reuse_are_off(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        job.spec.retain_built_usd = False
        job.spec.reuse_retained_usd = False

        self.assertEqual(window._retained_usd_status_text(job, None), "Build on next render")

    def test_status_text_requires_single_process_render_when_disabled(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        job.spec.render_all_frames_single_process = False
        job.spec.retain_built_usd = True

        self.assertEqual(window._retained_usd_status_text(job, None), "Requires single-process render")

    def test_invalid_reason_requires_single_process_render_when_retain_usd_is_enabled(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")
        job.spec.render_all_frames_single_process = False
        job.spec.retain_built_usd = True

        self.assertEqual(
            window._retained_usd_invalid_reason(job),
            "Cannot retain or reuse USD: enable Render All Frames with a Single Process.",
        )

    def test_hip_stale_reason_uses_metadata_hip_mtime(self) -> None:
        window = self._make_window()
        with tempfile.TemporaryDirectory() as tmpdir:
            hip_path = Path(tmpdir) / "scene.hip"
            hip_path.write_text("hip")
            current_mtime = hip_path.stat().st_mtime
            job = RenderJob(str(hip_path), "/stage/usdrender", "override")

            self.assertEqual(
                window._retained_usd_hip_stale_reason(job, {"hip_mtime": current_mtime - 10}),
                "Potentially stale: HIP was saved after this USD was built.",
            )

    def test_configured_retained_usd_folder_preview_uses_project_path(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/show/shot/test.hip", "/stage/Cam Top", "override")
        job.spec.usd_output_directory_mode = UsdOutputDirectoryMode.PROJECT_PATH

        self.assertEqual(
            window._configured_retained_usd_folder_preview(job),
            str(Path("E:/show/shot") / "usd_renders" / "test" / "Cam_Top_$RENDERID"),
        )

    def test_configured_retained_usd_folder_preview_uses_custom_path(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/show/shot/test.hip", "/stage/CamTop", "override")
        job.spec.usd_output_directory_mode = UsdOutputDirectoryMode.CUSTOM_PATH
        job.spec.usd_output_directory_custom_path = "D:/cache/usd"

        self.assertEqual(
            window._configured_retained_usd_folder_preview(job),
            str(Path("D:/cache/usd") / "CamTop_$RENDERID"),
        )

    def test_build_render_environment_deletes_existing_retained_usd_before_invalid_rebuild(self) -> None:
        window = self._make_window()
        deleted_jobs: list[str] = []
        synced_jobs: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=256, step=2)
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_reusable = False
        job.runtime.retained_usd_build_start_frame = 250
        job.runtime.retained_usd_build_end_frame = 256
        job.runtime.retained_usd_build_step = 2

        window._effective_device_mode_for_job = lambda target: DeviceOverrideMode.DEFAULT  # type: ignore[attr-defined]
        window._effective_device_selection_for_job = lambda target: ""  # type: ignore[attr-defined]
        window._sync_retained_usd_file_state = lambda target: synced_jobs.append(target.id)  # type: ignore[attr-defined]
        window._available_render_devices = lambda: []  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: "Cannot reuse USD: current frame range exceeds the built USD range."  # type: ignore[attr-defined]
        window._delete_retained_usd_folder_for_job = lambda target: deleted_jobs.append(target.id) or True  # type: ignore[attr-defined]
        window._append_log = lambda stream, text: None  # type: ignore[attr-defined]
        window._append_notification_message = lambda text, level: None  # type: ignore[attr-defined]

        env = window._build_render_environment(job)

        self.assertEqual(deleted_jobs, [job.id])
        self.assertEqual(env["HSRM_REUSE_EXISTING_USD"], "0")
        self.assertNotIn("HSRM_RETAIN_USD_OUTPUT_PATH", env)
        self.assertNotIn("HSRM_RETAIN_USD_OUTPUT_DIR", env)
        self.assertEqual(job.runtime.retained_usd_build_start_frame, 250)
        self.assertEqual(job.runtime.retained_usd_build_end_frame, 256)
        self.assertEqual(job.runtime.retained_usd_build_step, 2)
        self.assertGreaterEqual(len(synced_jobs), 2)

    def test_build_render_environment_sets_project_path_output_override(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=256, step=2)
        job.spec.render_all_frames_single_process = True
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = False
        job.spec.usd_output_directory_mode = UsdOutputDirectoryMode.PROJECT_PATH

        window._effective_device_mode_for_job = lambda target: DeviceOverrideMode.DEFAULT  # type: ignore[attr-defined]
        window._effective_device_selection_for_job = lambda target: ""  # type: ignore[attr-defined]
        window._sync_retained_usd_file_state = lambda target: None  # type: ignore[attr-defined]
        window._available_render_devices = lambda: []  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]
        window._append_log = lambda stream, text: None  # type: ignore[attr-defined]
        window._append_notification_message = lambda text, level: None  # type: ignore[attr-defined]

        env = window._build_render_environment(job)

        self.assertEqual(
            env["HSRM_RETAIN_USD_OUTPUT_DIR"],
            str(Path("E:/shot") / "usd_renders" / "test" / "usdrender_$RENDERID"),
        )
        self.assertEqual(env["HSRM_REUSE_EXISTING_USD"], "0")

    def test_build_render_environment_disables_retained_usd_when_single_process_render_is_off(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=256, step=2)
        job.spec.render_all_frames_single_process = False
        job.spec.retain_built_usd = True
        job.spec.reuse_retained_usd = True
        job.runtime.retained_usd_path = "E:/cache/scene.usd"

        window._effective_device_mode_for_job = lambda target: DeviceOverrideMode.DEFAULT  # type: ignore[attr-defined]
        window._effective_device_selection_for_job = lambda target: ""  # type: ignore[attr-defined]
        window._sync_retained_usd_file_state = lambda target: None  # type: ignore[attr-defined]
        window._available_render_devices = lambda: []  # type: ignore[attr-defined]
        window._append_log = lambda stream, text: None  # type: ignore[attr-defined]
        window._append_notification_message = lambda text, level: None  # type: ignore[attr-defined]

        env = window._build_render_environment(job)

        self.assertEqual(env["HSRM_RENDER_ALL_FRAMES_SINGLE_PROCESS"], "0")
        self.assertEqual(env["HSRM_RETAIN_USD_ENABLED"], "0")
        self.assertNotIn("HSRM_REUSE_EXISTING_USD", env)

    def test_current_retained_usd_build_range_uses_current_job_settings_not_previous_metadata(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=256, step=2)
        job.runtime.retained_usd_build_start_frame = 250
        job.runtime.retained_usd_build_end_frame = 255
        job.runtime.retained_usd_build_step = 1
        window._resolve_job_range_for_execution = lambda target, mutate_job=False: (250, 256, 2)  # type: ignore[attr-defined]

        self.assertEqual(window._current_retained_usd_build_range(job), (250, 256, 2))

    def test_reuse_range_ignores_stale_chunk_runtime_when_job_is_not_running(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=256, step=2, status=JobStatus.DONE)
        job.runtime.chunk_start_frame_runtime = 250
        job.runtime.chunk_end_frame_runtime = 255
        job.runtime.chunk_step_runtime = 1
        job.runtime.chunk_total_runtime = 1
        window._resolve_job_range_for_execution = lambda target, mutate_job=False: (250, 256, 2)  # type: ignore[attr-defined]

        self.assertEqual(window._current_retained_usd_reuse_range(job), (250, 256, 2))

    def test_sync_retained_usd_does_not_rewrite_metadata_when_reusing_existing_usd(self) -> None:
        window = self._make_window()
        writes: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=255, step=1, status=JobStatus.DONE)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = __file__
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_metadata_pending_write = False
        window._write_retained_usd_metadata = lambda target, path: writes.append(str(path))  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]

        window._sync_retained_usd_file_state(job)

        self.assertEqual(writes, [])

    def test_sync_retained_usd_writes_metadata_when_new_usd_was_built(self) -> None:
        window = self._make_window()
        writes: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=255, step=1, status=JobStatus.DONE)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = __file__
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_metadata_pending_write = True
        window._write_retained_usd_metadata = lambda target, path: writes.append(str(path))  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]

        window._sync_retained_usd_file_state(job)

        self.assertEqual(writes, [__file__])
        self.assertFalse(job.runtime.retained_usd_metadata_pending_write)

    def test_sync_retained_usd_does_not_write_metadata_while_build_is_still_running(self) -> None:
        window = self._make_window()
        writes: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=255, step=1, status=JobStatus.RUNNING)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = __file__
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_metadata_pending_write = True
        window._write_retained_usd_metadata = lambda target, path: writes.append(str(path))  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]

        window._sync_retained_usd_file_state(job)

        self.assertEqual(writes, [])
        self.assertTrue(job.runtime.retained_usd_metadata_pending_write)

    def test_sync_retained_usd_writes_metadata_when_build_finished_and_render_has_started(self) -> None:
        window = self._make_window()
        writes: list[str] = []
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=255, step=1, status=JobStatus.RUNNING)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_path = __file__
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_metadata_pending_write = True
        job.view.build_pass_completed = True
        job.view.phase_text = "Render"
        window._write_retained_usd_metadata = lambda target, path: writes.append(str(path))  # type: ignore[attr-defined]
        window._retained_usd_invalid_reason = lambda target: ""  # type: ignore[attr-defined]

        window._sync_retained_usd_file_state(job)

        self.assertEqual(writes, [__file__])
        self.assertFalse(job.runtime.retained_usd_metadata_pending_write)

    def test_sync_retained_usd_keeps_pending_write_while_waiting_for_new_path(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", start_frame=250, end_frame=255, step=1, status=JobStatus.RUNNING)
        job.spec.render_all_frames_single_process = True
        job.runtime.retained_usd_verified = True
        job.runtime.retained_usd_path = ""
        job.runtime.retained_usd_metadata_pending_write = True

        window._sync_retained_usd_file_state(job)

        self.assertTrue(job.runtime.retained_usd_metadata_pending_write)
        self.assertFalse(job.runtime.retained_usd_exists)

    def test_delete_retained_usd_folder_rejects_relative_path(self) -> None:
        window = self._make_window()
        job = RenderJob("E:/shot/test.hip", "/stage/usdrender", "override", status=JobStatus.CANCELED)
        job.runtime.retained_usd_path = "__render__.usd"
        messages: list[str] = []

        window._append_log = lambda stream, text: messages.append(text)  # type: ignore[attr-defined]

        self.assertFalse(window._delete_retained_usd_folder_for_job(job))
        self.assertEqual(job.runtime.retained_usd_path, "")
        self.assertTrue(any("Ignoring non-absolute retained USD path" in text for text in messages))


if __name__ == "__main__":
    unittest.main()
