from __future__ import annotations

import os
import re
import subprocess
import sys
import traceback
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from PySide6 import QtCore, QtGui, QtWidgets
from action_policy import (
    can_duplicate_jobs,
    can_edit_job,
    can_edit_job_column,
    can_open_queue_file,
    can_open_output_folder,
    can_preview_job,
    can_reload_jobs_from_file,
    can_remove_jobs,
    can_resume_job_from_output,
    is_job_runnable,
)
from atomic_io import read_json_file, write_json_atomic
from diagnostics import DiagnosticsSnapshot, build_diagnostics_report
from houdini_service import (
    build_render_preflight_script as build_render_preflight_script_model,
    ensure_husk_hook_files as ensure_husk_hook_files_model,
    load_houdini_script_text as load_houdini_script_text_model,
    project_houdini_scripts_dir as project_houdini_scripts_dir_model,
    required_houdini_script_filenames as required_houdini_script_filenames_model,
    validate_houdini_script_files as validate_houdini_script_files_model,
)
from job_validation import (
    validate_log_file_deletion,
    validate_logs_folder_access,
    validate_output_folder_open,
    validate_preview_launch,
    validate_render_missing_inputs,
    validate_render_missing_probe_path,
    validate_resolved_frame_range_for_resume,
    validate_resume_from_output_inputs,
    validate_resume_probe_path,
)
from queue_editing import (
    clear_job_resume_runtime_state as clear_job_resume_runtime_state_model,
    mark_job_offline as mark_job_offline_model,
    reset_job_state as reset_job_state_model,
    restore_job_online_status as restore_job_online_status_model,
)
from queue_file_controller import QueueFileController, QueueFileControllerHooks
from queue_models import DeviceOverrideMode, FrameHandlingMode, JobStatus, RenderJob, UsdOutputDirectoryMode
from queue_persistence import (
    apply_job_order as apply_job_order_model,
    apply_job_states as apply_job_states_model,
    insert_jobs_from_entries as insert_jobs_from_entries_model,
    job_from_persisted_dict as job_from_persisted_dict_model,
    job_states_for_ids as job_states_for_ids_model,
    job_to_persisted_dict as job_to_persisted_dict_model,
    load_queue_payload,
    queue_view_to_persisted_dict as queue_view_to_persisted_dict_model,
    remove_jobs_by_ids as remove_jobs_by_ids_model,
    save_queue_payload,
)
from queue_execution import (
    advance_job_to_next_chunk as advance_job_to_next_chunk_model,
    retry_current_chunk as retry_current_chunk_model,
)
from queue_filter_proxy import QueueFilterProxyModel, QUEUE_STATUS_FILTER_OPTIONS
from queue_cell_editing import apply_queue_cell_edit as apply_queue_cell_edit_model
from queue_run_executor import (
    handle_render_worker_crash as handle_render_worker_crash_model,
    handle_render_worker_message as handle_render_worker_message_model,
    on_render_finished as on_render_finished_model,
    start_job_runtime as start_job_runtime_model,
    update_job_progress_from_output as update_job_progress_from_output_model,
)
from queue_runtime_state import (
    format_duration_short as format_duration_short_model,
    initialize_job_chunk_runtime as initialize_job_chunk_runtime_model,
    job_end_time_display as job_end_time_display_model,
    job_frame_display as job_frame_display_model,
    job_started_time_display as job_started_time_display_model,
    job_time_remaining_display as job_time_remaining_display_model,
    job_total_time_display as job_total_time_display_model,
    reset_job_process_attempt_state as reset_job_process_attempt_state_model,
    total_frames_for_job as total_frames_for_job_model,
    update_job_render_timing_stats as update_job_render_timing_stats_model,
)
from queue_lifecycle import (
    QueueLifecycleState,
    decide_next_job as decide_next_job_model,
    evaluate_start_request as evaluate_start_request_model,
    with_pause_toggled as with_pause_toggled_model,
    with_queue_finished as with_queue_finished_model,
    with_queue_resumed as with_queue_resumed_model,
    with_queue_started as with_queue_started_model,
    with_stop_requested as with_stop_requested_model,
)
from queue_run_reporting import build_queue_run_summary as build_queue_run_summary_model
from queue_run_reporting import write_queue_snapshot as write_queue_snapshot_model
from queue_path_sync_tasks import (
    enqueue_path_sync_task as enqueue_path_sync_task_model,
    run_next_path_sync_task as run_next_path_sync_task_model,
    should_schedule_next_path_sync_task as should_schedule_next_path_sync_task_model,
)
from queue_path_sync_lock import (
    advance_path_sync_overlay as advance_path_sync_overlay_model,
    begin_path_sync_lock as begin_path_sync_lock_model,
    end_path_sync_lock as end_path_sync_lock_model,
    is_job_path_sync_locked as is_job_path_sync_locked_model,
)
from queue_header_grouping import (
    is_valid_queue_header_grouping as is_valid_queue_header_grouping_model,
    queue_column_widths_from_data as queue_column_widths_from_data_model,
    queue_header_visual_order as queue_header_visual_order_model,
    queue_hidden_columns_from_data as queue_hidden_columns_from_data_model,
)
from queue_output_paths import (
    frame_sequence_path_for_frame as frame_sequence_path_for_frame_model,
    normalize_output_display_path as normalize_output_display_path_model,
    output_folder_from_value as output_folder_from_value_model,
)
from queue_frame_scan import (
    first_missing_frame_and_contiguous_done as first_missing_frame_and_contiguous_done_model,
    missing_frame_runs_and_existing_count as missing_frame_runs_and_existing_count_model,
)
from queue_history import (
    apply_history_command as apply_history_command_model,
    bounded_undo_stack as bounded_undo_stack_model,
    history_command_targets_job as history_command_targets_job_model,
    should_push_history_command as should_push_history_command_model,
)
from queue_progress_state import (
    job_phase_display as job_phase_display_model,
    parse_percent_value as parse_percent_value_model,
    queue_progress_split_values as queue_progress_split_values_model,
)
from usd_queue_status import (
    usd_status_display as usd_status_display_model,
    usd_status_tooltip as usd_status_tooltip_model,
)
from queue_table_model import QueueTableModel, QueueTableModelHooks
from queue_tree_ui import (
    TREE_HIP_ROLE,
    TREE_KIND_ROLE,
    TREE_ROP_ROLE,
    build_queue_tree_panel as build_queue_tree_panel_model,
    refresh_queue_tree_model,
)
from queue_tree_sync import (
    propagate_hip_path_change as propagate_hip_path_change_model,
    propagate_rop_path_change as propagate_rop_path_change_model,
    refresh_jobs_from_rop_metadata as refresh_jobs_from_rop_metadata_model,
    sync_jobs_after_path_change as sync_jobs_after_path_change_model,
    validate_queue_path_value as validate_queue_path_value_model,
)
from queue_path_change_orchestration import (
    affected_job_ids_for_hip_path_change as affected_job_ids_for_hip_path_change_model,
    affected_job_ids_for_rop_path_change as affected_job_ids_for_rop_path_change_model,
    apply_hip_path_change_immediately as apply_hip_path_change_immediately_model,
    apply_rop_path_change_immediately as apply_rop_path_change_immediately_model,
    defer_finalize_path_change as defer_finalize_path_change_model,
    defer_reload_jobs_from_file as defer_reload_jobs_from_file_model,
)
from job_properties_actions import (
    JobPropertyEditSpec,
    device_mode_edit_spec as device_mode_edit_spec_model,
    device_selection_edit_spec as device_selection_edit_spec_model,
    retain_built_usd_edit_spec as retain_built_usd_edit_spec_model,
    reuse_retained_usd_edit_spec as reuse_retained_usd_edit_spec_model,
    single_process_render_edit_spec as single_process_render_edit_spec_model,
    usd_output_directory_custom_path_edit_spec as usd_output_directory_custom_path_edit_spec_model,
    usd_output_directory_mode_edit_spec as usd_output_directory_mode_edit_spec_model,
)
from job_properties_state import (
    build_job_properties_panel_state as build_job_properties_panel_state_model,
    default_job_properties_panel_state as default_job_properties_panel_state_model,
)
from render_session import RenderSessionController, RenderSessionHooks
from recovery_reporting import build_startup_recovery_summary
from scan_coordinator import ScanCoordinator, ScanCoordinatorHooks
from render_output_parser import (
    detect_phase_from_output_with_job as detect_phase_from_output_with_job_model,
)
from rop_metadata import (
    RopInfo,
    apply_rop_info_to_job as apply_rop_info_to_job_model,
)
from retained_usd_policy import (
    retained_usd_build_info as retained_usd_build_info_model,
    retained_usd_built_at_text as retained_usd_built_at_text_model,
    retained_usd_hip_stale_reason as retained_usd_hip_stale_reason_model,
    retained_usd_invalid_reason as retained_usd_invalid_reason_model,
    retained_usd_metadata_path as retained_usd_metadata_path_model,
    retained_usd_status_text as retained_usd_status_text_model,
)
from retained_usd_runtime import (
    clear_retained_usd_runtime as clear_retained_usd_runtime_model,
    delete_retained_usd_folder_for_job as delete_retained_usd_folder_for_job_model,
    is_absolute_retained_usd_path as is_absolute_retained_usd_path_model,
    selected_retained_usd_paths as selected_retained_usd_paths_model,
    should_write_retained_usd_metadata_now as should_write_retained_usd_metadata_now_model,
    sync_retained_usd_file_state as sync_retained_usd_file_state_model,
    write_retained_usd_metadata as write_retained_usd_metadata_model,
)
from retained_usd_panel_state import (
    can_delete_retained_usd as can_delete_retained_usd_model,
    multi_job_retained_usd_panel_state as multi_job_retained_usd_panel_state_model,
    retained_usd_panel_default_fields as retained_usd_panel_default_fields_model,
    single_job_retained_usd_panel_state as single_job_retained_usd_panel_state_model,
)
from retained_usd_actions import (
    clear_deleted_retained_usd_runtime as clear_deleted_retained_usd_runtime_model,
    delete_retained_usd_directories as delete_retained_usd_directories_model,
    first_retained_usd_folder as first_retained_usd_folder_model,
)
from notification_rules import (
    classified_render_error_notification as classified_render_error_notification_model,
    notification_messages_for_log as notification_messages_for_log_model,
    notification_summary_for_line as notification_summary_for_line_model,
)
from notification_list_state import (
    normalized_notification as normalized_notification_model,
    notification_color_hex as notification_color_hex_model,
    notification_signature as notification_signature_model,
    should_add_notification as should_add_notification_model,
    trim_notification_count as trim_notification_count_model,
)
from queue_selection_helpers import (
    mixed_value as mixed_value_model,
    selected_row_from_view_rows as selected_row_from_view_rows_model,
    source_rows_from_view_rows as source_rows_from_view_rows_model,
)
from queue_refresh_defer import (
    next_pending_refresh_action as next_pending_refresh_action_model,
    pending_refresh_args as pending_refresh_args_model,
    should_defer_queue_refresh as should_defer_queue_refresh_model,
)
from queue_undo_redo import pop_history_for_shortcut as pop_history_for_shortcut_model
from theme_support import DEFAULT_THEME, build_app_stylesheet, ensure_theme_icons, normalize_theme_colors
from widgets import AddJobPanel, CleanStepSpinBox, JobPropertiesPanel, PanelFrame, PreferencesDialog, QueueTableItemDelegate, QueueTableWidget, RopListWidget
from worker_client import RenderWorkerClient, ScanWorkerClient


APP_NAME = "Houdini Simple Render Manager"
ORG_NAME = "LocalOnly"
CONFIG_DIR_NAME = "HoudiniSimpleRenderManager"
CONFIG_FILE_NAME = "config.json"
THEME_FILE_NAME = "theme.json"
HOUDINI_SCRIPTS_DIR_NAME = "houdini_scripts"
LOGGER = logging.getLogger(__name__)

DEFAULT_QUEUE_COLUMN_WIDTHS: dict[int, int] = {
    QueueTableModel.NAME_COLUMN: 220,
    QueueTableModel.HIP_COLUMN: 260,
    QueueTableModel.ROP_COLUMN: 220,
    QueueTableModel.FRAME_RANGE_COLUMN: 95,
    QueueTableModel.STEP_COLUMN: 70,
    QueueTableModel.FRAME_HANDLING_COLUMN: 170,
    QueueTableModel.STATUS_COLUMN: 80,
    QueueTableModel.PROGRESS_COLUMN: 110,
    QueueTableModel.PHASE_COLUMN: 90,
    QueueTableModel.USD_COLUMN: 92,
    QueueTableModel.REMAINING_COLUMN: 100,
    QueueTableModel.FRAME_COLUMN: 95,
    QueueTableModel.FRAME_TIME_COLUMN: 85,
    QueueTableModel.AVG_FRAME_TIME_COLUMN: 90,
    QueueTableModel.STARTED_COLUMN: 90,
    QueueTableModel.COMPLETED_COLUMN: 100,
    QueueTableModel.RENDER_TIME_COLUMN: 100,
    QueueTableModel.OUTPUT_COLUMN: 260,
}


def _log_suppressed_exception(context: str, exc: Exception) -> None:
    LOGGER.debug("%s: %s", context, exc, exc_info=True)


class ConfigStore:
    def __init__(self) -> None:
        self.base_dir = get_appdata_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.base_dir / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.hooks_dir = self.base_dir / "hooks"
        self.hooks_dir.mkdir(parents=True, exist_ok=True)
        self.icons_dir = self.base_dir / "icons"
        self.icons_dir.mkdir(parents=True, exist_ok=True)
        self.queue_path = self.base_dir / "queue.json"
        self.theme_path = self.base_dir / THEME_FILE_NAME
        self.path = self.base_dir / CONFIG_FILE_NAME
        self.data: dict[str, Any] = {
            "hbatch_path": "",
            "player_path": "",
            "last_queue_path": "",
            "last_hip_dir": "",
            "recent_hip_paths": [],
            "recent_rop_paths": [],
            "experimental_chunking_enabled": False,
            "default_chunking_enabled": False,
            "default_chunk_size": 10,
            "default_retry_count": 1,
            "default_retry_delay": 5,
            "default_device_mode": DeviceOverrideMode.DEFAULT.value,
            "default_device_selection": "",
            "default_retain_built_usd": False,
            "default_usd_output_directory_mode": UsdOutputDirectoryMode.DEFAULT_TEMP.value,
            "default_usd_output_directory_custom_path": "",
        }
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = read_json_file(self.path)
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except Exception as exc:
            _log_suppressed_exception("ConfigStore.load", exc)

    def save(self) -> None:
        try:
            write_json_atomic(self.path, self.data)
        except Exception as exc:
            _log_suppressed_exception("ConfigStore.save", exc)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.save()

    def push_recent(self, key: str, value: str, max_items: int = 10) -> None:
        value = (value or "").strip()
        if not value:
            return
        items = [str(v) for v in self.data.get(key, []) if str(v).strip()]
        items = [v for v in items if os.path.normcase(v) != os.path.normcase(value)]
        items.insert(0, value)
        self.data[key] = items[:max_items]
        self.save()

    def new_job_log_path(self, job_name: str) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", job_name).strip("_") or "job"
        return self.logs_dir / f"{stamp}_{safe_name}.log"

    def hook_script_path(self, name: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "hook"
        return self.hooks_dir / f"{safe_name}.py"

    def load_theme(self) -> dict[str, str]:
        theme = dict(DEFAULT_THEME)
        if not self.theme_path.exists():
            return theme
        try:
            loaded = read_json_file(self.theme_path)
            if isinstance(loaded, dict):
                for key, value in loaded.items():
                    if key in theme and isinstance(value, str) and value.strip():
                        theme[key] = value.strip()
        except Exception as exc:
            _log_suppressed_exception("ConfigStore.load_theme", exc)
        return theme

    def save_theme(self, theme: dict[str, str]) -> None:
        try:
            payload = {k: str(v) for k, v in theme.items() if k in DEFAULT_THEME}
            write_json_atomic(self.theme_path, payload)
        except Exception as exc:
            _log_suppressed_exception("ConfigStore.save_theme", exc)


def get_appdata_dir() -> Path:
    appdata = os.environ.get("APPDATA")
    base_dir = Path(appdata) if appdata else Path.home() / "AppData" / "Roaming"
    return base_dir / CONFIG_DIR_NAME


def discover_hbatch() -> str:
    candidates: list[Path] = []

    hfs = os.environ.get("HFS", "").strip()
    if hfs:
        p = Path(hfs) / "bin" / "hbatch.exe"
        if p.exists():
            candidates.append(p)

    roots = [
        Path(r"C:\Program Files\Side Effects Software"),
        Path(r"C:\Program Files (x86)\Side Effects Software"),
    ]
    for root in roots:
        if not root.exists():
            continue
        for p in root.glob(r"Houdini*\bin\hbatch.exe"):
            if p.exists():
                candidates.append(p)

    if not candidates:
        return ""
    candidates = sorted({c.resolve() for c in candidates}, key=lambda x: str(x).lower(), reverse=True)
    return str(candidates[0])


def hscript_quote(value: str) -> str:
    escaped = value.replace("\\", "/").replace('"', r"\"")
    return f'"{escaped}"'


def safe_message(parent: QtWidgets.QWidget, title: str, text: str, details: str | None = None) -> None:
    box = QtWidgets.QMessageBox(parent)
    box.setWindowTitle(title)
    box.setIcon(QtWidgets.QMessageBox.Icon.Warning)
    box.setText(text)
    if details:
        box.setDetailedText(details)
    box.exec()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(self._build_app_icon())
        self.resize(1280, 820)
        self.setAcceptDrops(True)

        self.config = ConfigStore()
        self.theme = self.config.load_theme()
        self._hbatch_path = ""
        self.jobs: list[RenderJob] = []

        self.current_job_id: str | None = None
        self.current_job_log_handle = None
        self.queue_active = False
        self.queue_paused = False
        self.stop_requested = False
        self.canceling_current_job = False
        self._pending_kill_timer: QtCore.QTimer | None = None
        self._phase_promote_timer: QtCore.QTimer | None = None
        self._phase_promote_job_id: str | None = None
        self._queue_rerun_statuses: set[JobStatus] = set()
        self._jobs_started_this_run: set[str] = set()
        self._queue_next_search_index: int = 0

        self.log_entries: list[tuple[str, str]] = []
        self._last_notification_signature: tuple[str, str] | None = None
        self._active_render_request_id = ""
        self._active_scan_request_id = ""
        self._create_job_scan_in_progress = False
        self._render_finished_message_received = False
        self._active_hbatch_pid = 0
        self._pending_queue_refresh_args: dict[str, Any] | None = None
        self._pending_queue_refresh_timer = QtCore.QTimer(self)
        self._pending_queue_refresh_timer.setSingleShot(True)
        self._pending_queue_refresh_timer.timeout.connect(self._flush_pending_queue_refresh)
        self._path_sync_overlay_progress = 0.0
        self._path_sync_overlay_timer = QtCore.QTimer(self)
        self._path_sync_overlay_timer.setInterval(40)
        self._path_sync_overlay_timer.timeout.connect(self._advance_path_sync_overlay)
        self._houdini_scripts_missing_warned = False
        self._queue_header_group_restore_guard = False
        self._queue_header_valid_order: list[int] = []
        self._interaction_block_depth = 0
        self._path_sync_lock_counts: dict[str, int] = {}
        self._pending_path_sync_tasks: list[dict[str, Any]] = []
        self._path_sync_task_active = False
        self._undo_stack: list[dict[str, Any]] = []
        self._redo_stack: list[dict[str, Any]] = []
        self._history_applying = False
        self._status_default_message = "Ready"
        self._status_clear_timer = QtCore.QTimer(self)
        self._status_clear_timer.setSingleShot(True)
        self._status_clear_timer.timeout.connect(lambda: self._set_status_message(self._status_default_message))
        self._main_splitter_left_width_pref: int | None = None
        self._main_splitter_left_collapsed = False
        self._applying_main_splitter_width = False
        self._left_notifications_height_pref: int | None = None
        self._last_recovery_headline = ""
        self.scan_worker_client = ScanWorkerClient(
            worker_python_path=self._worker_python_path(),
            worker_script_path=self._worker_script_path("scan_worker.py"),
            parent=self,
        )
        self.background_scan_worker_client = ScanWorkerClient(
            worker_python_path=self._worker_python_path(),
            worker_script_path=self._worker_script_path("scan_worker.py"),
            parent=self,
        )
        self.render_worker_client = RenderWorkerClient(
            worker_python_path=self._worker_python_path(),
            worker_script_path=self._worker_script_path("render_worker.py"),
            parent=self,
        )
        self.scan_worker_client.message_received.connect(self._handle_scan_worker_message)
        self.scan_worker_client.stderr_received.connect(self._on_scan_worker_stderr)
        self.scan_worker_client.worker_failed.connect(self._on_scan_worker_failed)
        self.background_scan_worker_client.stderr_received.connect(self._on_background_scan_worker_stderr)
        self.background_scan_worker_client.worker_failed.connect(self._on_background_scan_worker_failed)
        self.render_worker_client.message_received.connect(self._handle_render_worker_message)
        self.render_worker_client.stderr_received.connect(self._append_render_worker_stderr)
        self.render_worker_client.worker_failed.connect(self._on_render_worker_failed)
        self.scan_coordinator = ScanCoordinator(
            ScanCoordinatorHooks(
                current_hbatch_path=self._current_hbatch_path,
                project_houdini_scripts_dir=self._project_houdini_scripts_dir,
                hooks_dir_path=lambda: self.config.hooks_dir,
                hbatch_exists=self._hbatch_exists,
                scan_in_progress=lambda: self._create_job_scan_in_progress,
                send_scan_request=self._send_scan_worker_request,
                request_scan_sync=lambda message_type, payload, timeout_ms: self._request_background_scan_worker_sync(
                    message_type,
                    payload,
                    timeout_ms=timeout_ms,
                ),
                append_log=self._append_log,
                safe_message=lambda title, text, details=None: safe_message(self, title, text, details),
                set_status_message=self._set_status_message,
                normalize_output_display_path=self._normalize_output_display_path,
                set_scan_hip_path_requested=lambda hip_path: setattr(self, "_scan_hip_path_requested", hip_path),
            )
        )
        self.queue_file_controller = QueueFileController(
            APP_NAME,
            QueueFileControllerHooks(
                config_get=lambda key, default=None: self.config.get(key, default),
                config_set=self.config.set,
                default_queue_path=lambda: self.config.queue_path,
                base_dir_path=lambda: self.config.base_dir,
                queue_active=lambda: self.queue_active,
                render_job_active=self._render_job_active,
                scan_in_progress=self._scan_in_progress,
                safe_message=lambda title, text, details=None: safe_message(self, title, text, details),
                load_queue_from_path=self._load_queue_from_path,
                save_queue_state=self._save_queue_state,
                set_status_message=self._set_status_message,
                set_window_title=self.setWindowTitle,
            ),
        )
        self.render_session = RenderSessionController(
            RenderSessionHooks(
                append_log=self._append_log,
                write_job_log=self._write_job_log,
                close_current_job_log=self._close_current_job_log,
                save_queue_state=self._save_queue_state,
                refresh_job_row=self._refresh_job_row,
                refresh_queue_table=self._refresh_queue_table,
                safe_message=lambda title, text, details=None: safe_message(self, title, text, details),
                start_worker_render=self._start_render_worker_payload,
                ensure_husk_hook_files=self._ensure_husk_hook_files,
                build_render_preflight_script=self._build_render_preflight_script,
                current_hbatch_path=self._current_hbatch_path,
                build_render_environment=self._build_render_environment,
                normalize_output_display_path=self._normalize_output_display_path,
                hscript_quote=hscript_quote,
                current_time=datetime.now,
                update_job_render_timing_stats=self._update_job_render_timing_stats,
                update_phase_from_frame_sequence=self._update_phase_from_frame_sequence,
                update_job_phase_from_output=self._update_job_phase_from_output,
                cancel_phase_promote=self._cancel_phase_promote,
                mark_job_offline=self._mark_job_offline,
                sync_retained_usd_file_state=self._sync_retained_usd_file_state,
            ),
            hook_script_path_fn=self.config.hook_script_path,
            disable_husk_mplay_fn=lambda: bool(
                getattr(self, "chk_disable_husk_mplay", None) is not None and self.chk_disable_husk_mplay.isChecked()
            ),
        )

        self._build_ui()
        self._shortcut_undo = QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Z"), self)
        self._shortcut_undo.activated.connect(self._undo_queue_edit)
        self._shortcut_redo = QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Shift+Z"), self)
        self._shortcut_redo.activated.connect(self._redo_queue_edit)
        self._shortcut_duplicate = QtGui.QShortcut(QtGui.QKeySequence("Ctrl+D"), self)
        self._shortcut_duplicate.activated.connect(self._duplicate_selected_jobs)
        self._shortcut_delete = QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key.Key_Delete), self)
        self._shortcut_delete.activated.connect(self._remove_selected_job)
        self._shortcut_job_properties = QtGui.QShortcut(QtGui.QKeySequence("P"), self)
        self._shortcut_job_properties.setContext(QtCore.Qt.ShortcutContext.ApplicationShortcut)
        self._shortcut_job_properties.activated.connect(self._toggle_job_properties_panel)
        self._apply_theme()
        self._load_hbatch_path()
        self._validate_houdini_script_files()
        self._load_persisted_queue()
        self._update_window_title()
        self._refresh_ui_state()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.main_splitter.addWidget(self._build_left_panel())
        self.main_splitter.addWidget(self._build_right_panel())
        self.main_splitter.setStretchFactor(0, 2)
        self.main_splitter.setStretchFactor(1, 3)
        self.main_splitter.splitterMoved.connect(self._on_main_splitter_moved)
        root.addWidget(self.main_splitter, stretch=1)
        root.addWidget(self._build_bottom_bar())
        self.statusBar().hide()
        self._set_status_message(self._status_default_message)
        saved_left_width = self.config.get("main_splitter_left_width")
        try:
            self._main_splitter_left_width_pref = int(saved_left_width) if saved_left_width is not None else None
        except (TypeError, ValueError):
            self._main_splitter_left_width_pref = None
        saved_top_height = self.config.get("left_splitter_top_height")
        try:
            self._left_splitter_top_height_pref = int(saved_top_height) if saved_top_height is not None else None
        except (TypeError, ValueError):
            self._left_splitter_top_height_pref = None
        saved_notifications_height = self.config.get("left_notifications_height")
        try:
            self._left_notifications_height_pref = int(saved_notifications_height) if saved_notifications_height is not None else None
        except (TypeError, ValueError):
            self._left_notifications_height_pref = None
        QtCore.QTimer.singleShot(0, self._apply_main_splitter_left_width_pref)
        QtCore.QTimer.singleShot(0, self._apply_left_splitter_default_sizes)

    def _begin_interaction_lock(self, status_text: str | None = None) -> None:
        self._interaction_block_depth += 1
        if self._interaction_block_depth != 1:
            return
        self._prepare_view_for_locked_refresh(getattr(self, "queue_table", None))
        self._prepare_view_for_locked_refresh(getattr(self, "queue_tree", None))
        central = self.centralWidget()
        if central is not None:
            central.setEnabled(False)
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
        if status_text:
            self._set_status_message(status_text)

    def _end_interaction_lock(self) -> None:
        if self._interaction_block_depth <= 0:
            self._interaction_block_depth = 0
            return
        self._interaction_block_depth -= 1
        if self._interaction_block_depth != 0:
            return
        central = self.centralWidget()
        if central is not None:
            central.setEnabled(True)
        while QtWidgets.QApplication.overrideCursor() is not None:
            QtWidgets.QApplication.restoreOverrideCursor()

    @staticmethod
    def _prepare_view_for_locked_refresh(view: QtWidgets.QAbstractItemView | None) -> None:
        if view is None:
            return
        cancel_inline_edit = getattr(view, "_cancel_inline_edit", None)
        if callable(cancel_inline_edit):
            try:
                cancel_inline_edit()
            except Exception as exc:
                _log_suppressed_exception("MainWindow._prepare_view_for_locked_refresh.cancel_inline_edit", exc)
        focus = QtWidgets.QApplication.focusWidget()
        try:
            if focus is not None and (focus is view or view.isAncestorOf(focus)):
                if isinstance(focus, QtWidgets.QWidget):
                    focus.clearFocus()
        except Exception as exc:
            _log_suppressed_exception("MainWindow._prepare_view_for_locked_refresh.focus_clear", exc)
        try:
            view.clearFocus()
        except Exception as exc:
            _log_suppressed_exception("MainWindow._prepare_view_for_locked_refresh.view_clear_focus", exc)

    @staticmethod
    def _dropped_hip_path(event: QtGui.QDropEvent | QtGui.QDragEnterEvent) -> str | None:
        mime = event.mimeData()
        if mime is None or not mime.hasUrls():
            return None
        for url in mime.urls():
            if not url.isLocalFile():
                continue
            path = url.toLocalFile().strip()
            if not path:
                continue
            if Path(path).suffix.lower() in {".hip", ".hiplc", ".hipnc"}:
                return path
        return None

    def _render_job_active(self) -> bool:
        return bool(self.current_job_id)

    def _scan_in_progress(self) -> bool:
        return bool(self._active_scan_request_id)

    def _is_active_job(self, job: RenderJob | None) -> bool:
        return bool(job is not None and self.current_job_id and job.id == self.current_job_id)

    def _is_job_path_sync_locked(self, job: RenderJob | str | None) -> bool:
        return is_job_path_sync_locked_model(self._path_sync_lock_counts, job)

    def _begin_path_sync_lock(self, job_ids: list[str]) -> None:
        locked_ids, started_overlay = begin_path_sync_lock_model(self._path_sync_lock_counts, job_ids)
        if not locked_ids:
            return
        if started_overlay:
            self._path_sync_overlay_progress = 0.0
            if hasattr(self, "queue_table"):
                self.queue_table.setProperty("pathSyncOverlayProgress", self._path_sync_overlay_progress)
            self._path_sync_overlay_timer.start()
        self.queue_table_model.refresh_jobs_by_id(locked_ids)
        self._refresh_ui_state()

    def _end_path_sync_lock(self, job_ids: list[str]) -> None:
        changed_ids, stopped_overlay = end_path_sync_lock_model(self._path_sync_lock_counts, job_ids)
        if not changed_ids:
            return
        if stopped_overlay:
            self._path_sync_overlay_timer.stop()
            self._path_sync_overlay_progress = 0.0
            if hasattr(self, "queue_table"):
                self.queue_table.setProperty("pathSyncOverlayProgress", self._path_sync_overlay_progress)
                self.queue_table.viewport().update()
        self.queue_table_model.refresh_jobs_by_id(changed_ids)
        self._refresh_queue_tree_view()
        self._refresh_ui_state()

    def _advance_path_sync_overlay(self) -> None:
        self._path_sync_overlay_progress, active = advance_path_sync_overlay_model(
            self._path_sync_lock_counts,
            self._path_sync_overlay_progress,
        )
        if not active:
            self._path_sync_overlay_timer.stop()
            if hasattr(self, "queue_table"):
                self.queue_table.setProperty("pathSyncOverlayProgress", self._path_sync_overlay_progress)
            return
        if hasattr(self, "queue_table"):
            self.queue_table.setProperty("pathSyncOverlayProgress", self._path_sync_overlay_progress)
            self.queue_table.viewport().update()

    def _enqueue_path_sync_task(self, task: dict[str, Any]) -> None:
        enqueue_path_sync_task_model(self._pending_path_sync_tasks, task)
        self._schedule_next_path_sync_task()

    def _schedule_next_path_sync_task(self) -> None:
        if not should_schedule_next_path_sync_task_model(
            path_sync_task_active=self._path_sync_task_active,
            pending_tasks=self._pending_path_sync_tasks,
        ):
            return
        self._path_sync_task_active = True
        QtCore.QTimer.singleShot(0, self._run_next_path_sync_task)

    def _run_next_path_sync_task(self) -> None:
        if not self._pending_path_sync_tasks:
            self._path_sync_task_active = False
            self._refresh_ui_state()
            return
        refresh_needed = False
        try:
            refresh_needed = run_next_path_sync_task_model(
                jobs=self.jobs,
                pending_tasks=self._pending_path_sync_tasks,
                offline_status=JobStatus.OFFLINE,
                refresh_queue_tree_view=self._refresh_queue_tree_view,
                refresh_jobs_from_rop_metadata=lambda hip_jobs, reset_override_to_rop: self._refresh_jobs_from_rop_metadata(
                    hip_jobs,
                    reset_override_to_rop=reset_override_to_rop,
                ),
                end_path_sync_lock=self._end_path_sync_lock,
                push_history_command=self._push_history_command,
                job_states_for_ids=self._job_states_for_ids,
                save_queue_state=self._save_queue_state,
                append_notification_message=self._append_notification_message,
            )
        finally:
            if refresh_needed:
                self._refresh_queue_table()
            self._path_sync_task_active = False
            self._schedule_next_path_sync_task()

    def _worker_script_path(self, filename: str) -> Path:
        return Path(__file__).resolve().with_name(filename)

    def _worker_python_path(self) -> str:
        return sys.executable or "python"

    def _send_scan_worker_request(self, message_type: str, payload: dict[str, Any]) -> bool:
        request_id = self.scan_worker_client.send_request(message_type, payload)
        self._active_scan_request_id = str(request_id or "")
        return bool(request_id)

    def _scan_worker_request_payload(self, *, hip_path: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.scan_coordinator.build_request_payload(hip_path=hip_path, extra=extra)

    def _request_scan_worker_sync(self, message_type: str, payload: dict[str, Any], *, timeout_ms: int = 30000) -> dict[str, Any] | None:
        self._active_scan_request_id = "__sync__"
        response = self.scan_worker_client.request_sync(message_type, payload, timeout_ms=timeout_ms)
        self._active_scan_request_id = ""
        return response

    def _request_background_scan_worker_sync(
        self,
        message_type: str,
        payload: dict[str, Any],
        *,
        timeout_ms: int = 30000,
    ) -> dict[str, Any] | None:
        return self.background_scan_worker_client.request_sync(message_type, payload, timeout_ms=timeout_ms)

    def _request_scan_worker_sync_payload(
        self,
        message_type: str,
        *,
        hip_path: str,
        timeout_ms: int = 30000,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        return self.scan_coordinator.request_sync_payload(
            message_type,
            hip_path=hip_path,
            timeout_ms=timeout_ms,
            extra=extra,
        )

    def _send_render_worker_request(self, message_type: str, payload: dict[str, Any], *, request_id: str | None = None) -> bool:
        send_request_id = self.render_worker_client.send_request(message_type, payload, request_id=request_id)
        if message_type == "render.start":
            self._active_render_request_id = str(send_request_id or "")
            self._render_finished_message_received = False
        return bool(send_request_id)

    def _start_render_worker_payload(self, payload: dict[str, Any]) -> bool:
        return self._send_render_worker_request("render.start", payload)

    def _on_scan_worker_stderr(self, text: str) -> None:
        if text:
            self._append_log("Stderr", text)

    def _on_scan_worker_failed(self, reason: str) -> None:
        self._active_scan_request_id = ""
        self._create_job_scan_in_progress = False
        safe_message(self, "Scan Worker", reason, self.scan_worker_client.last_stderr_text or None)
        self._set_status_message(reason, 5000)
        self._refresh_ui_state()

    def _on_background_scan_worker_stderr(self, text: str) -> None:
        if text:
            self._append_log("Stderr", text)

    def _on_background_scan_worker_failed(self, reason: str) -> None:
        self._append_log("Stderr", f"[Background Scan Worker] {reason}\n")
        self._set_status_message(reason, 5000)
        self._refresh_ui_state()

    def _append_render_worker_stderr(self, text: str) -> None:
        if text:
            self._append_log("Stderr", text)

    def _on_render_worker_failed(self, reason: str) -> None:
        if self._render_job_active() and not self._render_finished_message_received:
            self._handle_render_worker_crash(reason)

    def dragEnterEvent(self, event: QtGui.QDragEnterEvent) -> None:
        hip_path = self._dropped_hip_path(event)
        if hip_path and not self._scan_in_progress():
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dropEvent(self, event: QtGui.QDropEvent) -> None:
        hip_path = self._dropped_hip_path(event)
        if not hip_path:
            super().dropEvent(event)
            return
        if self._scan_in_progress():
            safe_message(self, "Scan In Progress", "Wait for the current scan to finish before loading another HIP file.")
            event.ignore()
            return
        if hasattr(self, "add_job_panel") and self.add_job_panel is not None:
            self.add_job_panel.load_hip_path(hip_path, request_scan=True)
            self._set_status_message(f"Loaded HIP: {Path(hip_path).name}", 3000)
            event.acceptProposedAction()
            return
        super().dropEvent(event)

    def _build_bottom_bar(self) -> QtWidgets.QWidget:
        bar = QtWidgets.QWidget()
        bar.setObjectName("bottomStatusBar")
        layout = QtWidgets.QGridLayout(bar)
        layout.setContentsMargins(0, 4, 0, 0)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(0)
        layout.setColumnStretch(0, 1)
        layout.setColumnStretch(1, 0)
        layout.setColumnStretch(2, 1)
        self.status_label = QtWidgets.QLabel(self._status_default_message)
        self.status_label.setObjectName("bottomStatusLabel")
        layout.addWidget(
            self.status_label,
            0,
            0,
            QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
        )
        self.legend_override_icon = QtWidgets.QLabel()
        self.legend_override_icon.setFixedSize(12, 12)
        self.legend_override_text = QtWidgets.QLabel("Override")
        self.legend_lock_icon = QtWidgets.QLabel()
        self.legend_lock_icon.setFixedSize(12, 12)
        self.legend_lock_text = QtWidgets.QLabel("Strict Range (can't be edited)")
        legend_layout = QtWidgets.QHBoxLayout()
        legend_layout.setContentsMargins(0, 0, 0, 0)
        legend_layout.setSpacing(6)
        legend_layout.addWidget(self.legend_override_icon, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        legend_layout.addWidget(self.legend_override_text, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        legend_layout.addSpacing(16)
        legend_layout.addWidget(self.legend_lock_icon, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        legend_layout.addWidget(self.legend_lock_text, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        legend_host = QtWidgets.QWidget()
        legend_host.setObjectName("transparentHost")
        legend_host.setLayout(legend_layout)
        layout.addWidget(
            legend_host,
            0,
            1,
            QtCore.Qt.AlignmentFlag.AlignHCenter | QtCore.Qt.AlignmentFlag.AlignVCenter,
        )
        self.btn_preferences = QtWidgets.QPushButton("Preferences")
        self.btn_preferences.clicked.connect(self._open_preferences_dialog)
        self.btn_preferences.setObjectName("statusPreferencesButton")
        layout.addWidget(
            self.btn_preferences,
            0,
            2,
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter,
        )
        self._update_bottom_legend_icons()
        return bar

    def _update_bottom_legend_icons(self) -> None:
        icon_paths = getattr(self, "_theme_icons", {}) or {}

        def _set_icon(label: QtWidgets.QLabel, key: str) -> None:
            path = str(icon_paths.get(key, "") or "")
            pixmap = QtGui.QPixmap(path) if path else QtGui.QPixmap()
            if not pixmap.isNull():
                label.setPixmap(
                    pixmap.scaled(
                        label.size(),
                        QtCore.Qt.AspectRatioMode.KeepAspectRatio,
                        QtCore.Qt.TransformationMode.SmoothTransformation,
                    )
                )
            else:
                label.clear()

        if hasattr(self, "legend_override_icon"):
            _set_icon(self.legend_override_icon, "override_dot_red")
        if hasattr(self, "legend_lock_icon"):
            _set_icon(self.legend_lock_icon, "lock_orange")

    def _queue_menu_icon(self) -> QtGui.QIcon:
        color = self.palette().color(QtGui.QPalette.ColorRole.ButtonText)
        pix = QtGui.QPixmap(36, 36)
        pix.fill(QtCore.Qt.GlobalColor.transparent)
        painter = QtGui.QPainter(pix)
        try:
            painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
            pen = QtGui.QPen(color)
            pen.setWidth(3)
            painter.setPen(pen)
            painter.drawLine(8, 10, 28, 10)
            painter.drawLine(8, 18, 28, 18)
            painter.drawLine(8, 26, 28, 26)
        finally:
            painter.end()
        return QtGui.QIcon(pix)

    @staticmethod
    def _build_app_icon() -> QtGui.QIcon:
        icon = QtGui.QIcon()
        for size in (16, 24, 32, 48, 64, 128, 256):
            pix = QtGui.QPixmap(size, size)
            pix.fill(QtGui.QColor("#000000"))
            painter = QtGui.QPainter(pix)
            try:
                painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
                pen = QtGui.QPen(QtGui.QColor("#ffffff"))
                pen.setWidth(max(2, size // 12))
                painter.setPen(pen)
                left = max(3, size // 5)
                right = size - left
                y1 = max(4, int(size * 0.28))
                y2 = int(size * 0.50)
                y3 = min(size - 4, int(size * 0.72))
                painter.drawLine(left, y1, right, y1)
                painter.drawLine(left, y2, right, y2)
                painter.drawLine(left, y3, right, y3)
            finally:
                painter.end()
            icon.addPixmap(pix)
        return icon

    def _set_status_message(self, text: str, timeout_ms: int | None = None) -> None:
        if hasattr(self, "status_label") and self.status_label is not None:
            self.status_label.setText(text or "")
        if timeout_ms and timeout_ms > 0:
            self._status_clear_timer.start(int(timeout_ms))
        else:
            self._status_clear_timer.stop()

    def _apply_left_splitter_default_sizes(self) -> None:
        splitter = getattr(self, "left_vertical_splitter", None)
        create_panel = getattr(self, "create_job_frame", None)
        if splitter is None or create_panel is None:
            return
        total_height = splitter.height()
        if total_height <= 0:
            total_height = splitter.sizeHint().height()
        if total_height <= 0:
            return
        preferred_top = getattr(self, "_left_splitter_top_height_pref", None)
        top_target = preferred_top if preferred_top is not None else max(
            create_panel.minimumSizeHint().height(),
            create_panel.sizeHint().height(),
        )
        top_target = max(1, min(top_target, max(1, total_height - 80)))
        bottom_target = max(1, total_height - top_target)
        splitter.setSizes([top_target, bottom_target])

    def _apply_left_column_splitter_default_sizes(self) -> None:
        splitter = getattr(self, "left_column_splitter", None)
        notifications_panel = getattr(self, "notifications_frame", None)
        if splitter is None or notifications_panel is None:
            return
        total_height = splitter.height()
        if total_height <= 0:
            total_height = splitter.sizeHint().height()
        if total_height <= 0:
            return
        preferred_bottom = getattr(self, "_left_notifications_height_pref", None)
        bottom_target = preferred_bottom if preferred_bottom is not None else max(
            notifications_panel.minimumSizeHint().height(),
            min(170, notifications_panel.sizeHint().height()),
        )
        bottom_target = max(80, min(bottom_target, max(80, total_height - 160)))
        top_target = max(120, total_height - bottom_target)
        splitter.setSizes([top_target, bottom_target])

    def _on_left_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "left_vertical_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) >= 2:
            self._left_splitter_top_height_pref = int(max(0, sizes[0]))

    def _on_left_column_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "left_column_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) >= 2:
            self._left_notifications_height_pref = int(max(0, sizes[1]))

    def _chunking_enabled(self) -> bool:
        return self._experimental_chunking_enabled() and bool(getattr(self, "chk_enable_chunking", None) and self.chk_enable_chunking.isChecked())

    def _chunk_size_value(self) -> int:
        try:
            return int(getattr(self, "spin_chunk_size", None).value())  # type: ignore[union-attr]
        except (AttributeError, TypeError, ValueError):
            return 1

    def _retry_count_value(self) -> int:
        try:
            return int(getattr(self, "spin_auto_retry", None).value())  # type: ignore[union-attr]
        except (AttributeError, TypeError, ValueError):
            return 0

    def _retry_delay_value(self) -> int:
        try:
            return int(getattr(self, "spin_retry_delay", None).value())  # type: ignore[union-attr]
        except (AttributeError, TypeError, ValueError):
            return 0

    def _default_chunking_enabled(self) -> bool:
        return bool(self.config.get("default_chunking_enabled", False))

    def _experimental_chunking_enabled(self) -> bool:
        return bool(self.config.get("experimental_chunking_enabled", False))

    def _default_chunk_size(self) -> int:
        try:
            return max(1, int(self.config.get("default_chunk_size", 10)))
        except (TypeError, ValueError):
            return 10

    def _default_retry_count(self) -> int:
        try:
            return max(0, int(self.config.get("default_retry_count", 1)))
        except (TypeError, ValueError):
            return 1

    def _default_retry_delay(self) -> int:
        try:
            return max(0, int(self.config.get("default_retry_delay", 5)))
        except (TypeError, ValueError):
            return 5

    def _default_device_mode(self) -> DeviceOverrideMode:
        return DeviceOverrideMode.coerce(self.config.get("default_device_mode", DeviceOverrideMode.DEFAULT.value))

    def _default_device_selection(self) -> str:
        return RenderJob.normalize_device_selection(self.config.get("default_device_selection", ""))

    def _default_retain_built_usd(self) -> bool:
        return bool(self.config.get("default_retain_built_usd", False))

    def _default_usd_output_directory_mode(self) -> UsdOutputDirectoryMode:
        return UsdOutputDirectoryMode.coerce(
            self.config.get("default_usd_output_directory_mode", UsdOutputDirectoryMode.DEFAULT_TEMP.value)
        )

    def _default_usd_output_directory_custom_path(self) -> str:
        return str(self.config.get("default_usd_output_directory_custom_path", "") or "").strip()

    @staticmethod
    def _is_virtual_or_integrated_gpu_name(name: str) -> bool:
        txt = str(name or "").strip().lower()
        if not txt:
            return True
        virtual_markers = (
            "virtual",
            "microsoft basic",
            "hyper-v",
            "vmware",
            "citrix",
            "remote",
            "parsec",
            "displaylink",
            "mirage",
            "swiftshader",
            "llvmpipe",
        )
        if any(marker in txt for marker in virtual_markers):
            return True
        discrete_markers = (
            "rtx",
            "gtx",
            "quadro",
            "tesla",
            "radeon rx",
            "radeon pro",
            "radeon vii",
            "arc ",
            "arc(tm)",
        )
        if any(marker in txt for marker in discrete_markers):
            return False
        integrated_patterns = (
            r"\bintel\b",
            r"\buhd\b",
            r"\biris\b",
            r"\bhd graphics\b",
            r"\bradeon(?:\(tm\))?\s+graphics\b",
            r"\bvega\s+graphics\b",
        )
        return any(re.search(pattern, txt) for pattern in integrated_patterns)

    @staticmethod
    def _device_brand_sort_key(name: str) -> tuple[int, str]:
        txt = str(name or "").strip().lower()
        if "nvidia" in txt or any(token in txt for token in ("rtx", "gtx", "quadro", "tesla")):
            return (0, txt)
        if "amd" in txt or "radeon" in txt:
            return (1, txt)
        if "intel" in txt or "arc" in txt:
            return (2, txt)
        return (3, txt)

    def _available_render_devices(self) -> list[dict[str, str]]:
        cached = getattr(self, "_render_device_infos", None)
        if cached is not None:
            return list(cached)
        devices: list[dict[str, str]] = []
        if os.name == "nt":
            try:
                cpu_name = ""
                try:
                    cpu_result = subprocess.run(
                        [
                            "powershell",
                            "-NoProfile",
                            "-Command",
                            "$ErrorActionPreference='SilentlyContinue'; Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=4,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    cpu_name = str(cpu_result.stdout or "").strip()
                except (OSError, subprocess.SubprocessError):
                    cpu_name = ""
                if cpu_name:
                    devices.append({"id": "cpu", "name": cpu_name})
                result = subprocess.run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-Command",
                        "$ErrorActionPreference='SilentlyContinue'; Get-CimInstance Win32_VideoController | Where-Object { $_.Name } | Select-Object -ExpandProperty Name",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=4,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                gpu_devices: list[dict[str, str]] = []
                seen_counts: dict[str, int] = {}
                gpu_index = 0
                for raw_line in result.stdout.splitlines():
                    name = str(raw_line or "").strip()
                    if not name:
                        continue
                    if self._is_virtual_or_integrated_gpu_name(name):
                        continue
                    count = int(seen_counts.get(name, 0)) + 1
                    seen_counts[name] = count
                    label = name if count == 1 else f"{name} ({count})"
                    gpu_devices.append({"id": str(gpu_index), "name": f"{label} ({gpu_index})"})
                    gpu_index += 1
                gpu_devices.sort(key=lambda item: self._device_brand_sort_key(str(item.get("name", ""))))
                devices.extend(gpu_devices)
            except (OSError, subprocess.SubprocessError):
                devices = []
        self._render_device_infos = list(devices)
        return list(devices)

    @staticmethod
    def _chunk_ranges_from_range(start: int, end: int, step: int, chunk_size: int) -> list[tuple[int, int, int]]:
        if step <= 0 or end < start:
            return []
        if chunk_size <= 0:
            chunk_size = 1
        frames = list(range(start, end + 1, step))
        if not frames:
            return []
        if len(frames) <= chunk_size:
            return [(start, end, step)]
        chunks: list[tuple[int, int, int]] = []
        for i in range(0, len(frames), chunk_size):
            sub = frames[i : i + chunk_size]
            chunks.append((int(sub[0]), int(sub[-1]), int(step)))
        return chunks

    def _expand_ranges_with_chunking(
        self,
        ranges: list[tuple[int, int, int]],
    ) -> list[tuple[int, int, int]]:
        if not ranges:
            return []
        if not self._chunking_enabled():
            return [(int(s), int(e), int(st)) for s, e, st in ranges]
        chunk_size = self._chunk_size_value()
        expanded: list[tuple[int, int, int]] = []
        for s, e, st in ranges:
            expanded.extend(self._chunk_ranges_from_range(int(s), int(e), int(st), chunk_size))
        return expanded

    def _job_phase_display(self, job: RenderJob) -> str:
        return job_phase_display_model(job)

    def _job_usd_status_display(self, job: RenderJob) -> str:
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        return usd_status_display_model(
            retained_path=retained_path,
            retained_exists=bool(job.runtime.retained_usd_exists),
            stale_reason=self._retained_usd_stale_reason(job),
        )

    def _job_usd_status_tooltip(self, job: RenderJob) -> str:
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        return usd_status_tooltip_model(
            retained_path=retained_path,
            retained_exists=bool(job.runtime.retained_usd_exists),
            stale_reason=self._retained_usd_stale_reason(job),
            reuse_retained_usd=bool(job.spec.reuse_retained_usd),
        )

    @staticmethod
    def _job_time_remaining_display(job: RenderJob) -> str:
        return job_time_remaining_display_model(
            job,
            {JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELED, JobStatus.INTERRUPTED, JobStatus.OFFLINE},
        )

    @staticmethod
    def _job_end_time_display(job: RenderJob) -> str:
        return job_end_time_display_model(job)

    @staticmethod
    def _job_started_time_display(job: RenderJob) -> str:
        return job_started_time_display_model(job)

    @staticmethod
    def _job_total_time_display(job: RenderJob) -> str:
        return job_total_time_display_model(job, now_fn=datetime.now)

    @staticmethod
    def _job_frame_display(job: RenderJob) -> str:
        return job_frame_display_model(
            job,
            {JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELED, JobStatus.INTERRUPTED, JobStatus.OFFLINE},
        )

    def _reset_job_process_attempt_state(self, job: RenderJob, *, preserve_output: bool = True) -> None:
        reset_job_process_attempt_state_model(job, preserve_output=preserve_output)

    def _initialize_job_chunk_runtime(
        self,
        job: RenderJob,
        *,
        forced_ranges: list[tuple[int, int, int]] | None = None,
    ) -> None:
        initialize_job_chunk_runtime_model(
            job,
            forced_ranges=forced_ranges,
            retry_count_value=self._retry_count_value(),
            resolve_job_range_for_execution=lambda target: self._resolve_job_range_for_execution(target, mutate_job=True),
            expand_ranges_with_chunking=self._expand_ranges_with_chunking,
        )

    def _start_job_process_continuation(self, job: RenderJob) -> None:
        result = self.render_session.start_job_continuation(
            job,
            current_job_id=self.current_job_id,
            stop_requested=self.stop_requested,
            canceling_current_job=self.canceling_current_job,
        )
        if result.needs_queue_advance:
            self.current_job_id = None
            self._maybe_start_next_job()

    def _advance_job_to_next_chunk(self, job: RenderJob) -> bool:
        return advance_job_to_next_chunk_model(job)

    def _retry_current_chunk(self, job: RenderJob) -> bool:
        return retry_current_chunk_model(job)

    def _on_main_splitter_moved(self, _pos: int, _index: int) -> None:
        if self._applying_main_splitter_width:
            return
        if not hasattr(self, "main_splitter") or self.main_splitter is None:
            return
        sizes = self.main_splitter.sizes()
        if len(sizes) >= 2:
            if sizes[0] <= 0:
                self._main_splitter_left_collapsed = True
            else:
                self._main_splitter_left_collapsed = False
                self._main_splitter_left_width_pref = int(sizes[0])

    def _apply_main_splitter_left_width_pref(self) -> None:
        if self._applying_main_splitter_width:
            return
        if not hasattr(self, "main_splitter") or self.main_splitter is None:
            return
        sizes = self.main_splitter.sizes()
        if len(sizes) < 2:
            return
        total = int(sum(max(0, s) for s in sizes))
        if total <= 0:
            return
        if self._main_splitter_left_collapsed:
            target_left = 0
            target_right = total
            if sizes[0] == target_left and sizes[1] == target_right:
                return
            self._applying_main_splitter_width = True
            try:
                self.main_splitter.setSizes([target_left, target_right])
            finally:
                self._applying_main_splitter_width = False
            return
        if self._main_splitter_left_width_pref is None:
            if sizes[0] > 0:
                self._main_splitter_left_width_pref = int(sizes[0])
            return
        left_widget = self.main_splitter.widget(0)
        right_widget = self.main_splitter.widget(1)
        left_min = int(left_widget.minimumWidth()) if left_widget is not None else 0
        right_min = int(right_widget.minimumWidth()) if right_widget is not None else 0
        target_left = max(left_min, min(int(self._main_splitter_left_width_pref), max(left_min, total - right_min)))
        target_right = max(0, total - target_left)
        if sizes[0] == target_left and sizes[1] == target_right:
            return
        self._applying_main_splitter_width = True
        try:
            self.main_splitter.setSizes([target_left, target_right])
        finally:
            self._applying_main_splitter_width = False

    def _build_job_create_panel(self) -> QtWidgets.QWidget:
        self.add_job_panel = AddJobPanel(self.config)
        self.add_job_panel.setObjectName("panelEmbeddedGroup")
        self.add_job_panel.setTitle("")
        self.add_job_panel.add_job_requested.connect(self._handle_add_job_requested)
        self.add_job_panel.scan_requested.connect(self._handle_scan_requested)
        self.create_job_frame = PanelFrame("Create Job", self.add_job_panel)
        return self.create_job_frame

    def _build_left_panel(self) -> QtWidgets.QWidget:
        host = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(host)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        top_host = QtWidgets.QWidget()
        top_layout = QtWidgets.QVBoxLayout(top_host)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(6)
        self.left_vertical_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.left_vertical_splitter.addWidget(self._build_job_create_panel())
        self.left_vertical_splitter.addWidget(self._build_tree_view_panel())
        self.left_vertical_splitter.setStretchFactor(0, 4)
        self.left_vertical_splitter.setStretchFactor(1, 2)
        self.left_vertical_splitter.splitterMoved.connect(self._on_left_splitter_moved)
        top_layout.addWidget(self.left_vertical_splitter, 1)

        self.left_column_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.left_column_splitter.addWidget(top_host)
        self.left_column_splitter.addWidget(self._build_notifications_panel())
        self.left_column_splitter.setStretchFactor(0, 5)
        self.left_column_splitter.setStretchFactor(1, 2)
        self.left_column_splitter.splitterMoved.connect(self._on_left_column_splitter_moved)
        layout.addWidget(self.left_column_splitter, 1)
        return host

    def _build_notifications_panel(self) -> QtWidgets.QWidget:
        box = QtWidgets.QGroupBox("Notifications")
        layout = QtWidgets.QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.notifications_list = RopListWidget()
        self.notifications_list.setObjectName("notificationsList")
        self.notifications_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.notifications_list.setAlternatingRowColors(True)
        self.notifications_list.setUniformItemSizes(False)
        self.notifications_list.setWordWrap(True)
        self.notifications_list.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.notifications_list.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.notifications_list.verticalScrollBar().setSingleStep(20)
        layout.addWidget(self.notifications_list, 1)

        footer_row = QtWidgets.QHBoxLayout()
        footer_row.setContentsMargins(8, 8, 8, 8)
        footer_row.setSpacing(8)
        self.btn_clear_notifications = QtWidgets.QPushButton("Clear")
        self.btn_clear_notifications.clicked.connect(self._clear_notifications_view_only)
        footer_row.addWidget(self.btn_clear_notifications)
        footer_row.addStretch(1)
        layout.addLayout(footer_row)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        self.notifications_frame = PanelFrame("Notifications", box)
        self.notifications_frame.set_body_margins(0, 0, 0, 0)
        return self.notifications_frame

    def _build_tree_view_panel(self) -> QtWidgets.QWidget:
        panel, self.queue_tree, self.queue_tree_model, self.btn_reload_all_tree = build_queue_tree_panel_model(
            self,
            item_changed_handler=self._on_queue_tree_item_changed,
        )
        self.btn_reload_all_tree.clicked.connect(self._reload_all_jobs_from_files)
        self.tree_view_frame = panel
        return panel

    def _build_queue_panel(self) -> QtWidgets.QWidget:
        box = QtWidgets.QGroupBox("Queue")
        layout = QtWidgets.QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        controls_row = QtWidgets.QHBoxLayout()
        controls_row.setContentsMargins(0, 0, 0, 0)
        controls_row.setSpacing(8)
        self.btn_start_queue = QtWidgets.QPushButton("Start")
        self.btn_start_queue.clicked.connect(self._start_queue)
        self.btn_pause_queue = QtWidgets.QPushButton("Pause")
        self.btn_pause_queue.clicked.connect(self._toggle_pause)
        self.btn_stop_queue = QtWidgets.QPushButton("Stop")
        self.btn_stop_queue.clicked.connect(self._stop_queue)
        self.chk_disable_husk_mplay = QtWidgets.QCheckBox("Disable MPlay monitor")
        self.chk_disable_husk_mplay.setChecked(True)
        self.chk_enable_chunking = QtWidgets.QCheckBox("Chunking")
        self.chk_enable_chunking.setChecked(self._default_chunking_enabled())
        self.chk_enable_chunking.toggled.connect(lambda _checked: self._refresh_ui_state())
        self.spin_chunk_size = CleanStepSpinBox()
        self.spin_chunk_size.setRange(1, 100000)
        self.spin_chunk_size.setValue(self._default_chunk_size())
        self.spin_chunk_size.setPrefix("Size ")
        self.spin_chunk_size.setFixedWidth(90)
        self.lbl_retry = QtWidgets.QLabel("Retry")
        self.spin_auto_retry = CleanStepSpinBox()
        self.spin_auto_retry.setRange(0, 20)
        self.spin_auto_retry.setValue(self._default_retry_count())
        self.spin_auto_retry.setFixedWidth(72)
        self.lbl_delay = QtWidgets.QLabel("Delay")
        self.spin_retry_delay = CleanStepSpinBox()
        self.spin_retry_delay.setRange(0, 3600)
        self.spin_retry_delay.setValue(self._default_retry_delay())
        self.spin_retry_delay.setFixedWidth(84)

        sep_buttons_chunk = QtWidgets.QFrame()
        sep_buttons_chunk.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep_buttons_chunk.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        sep_buttons_chunk.setObjectName("toolbarSeparator")
        sep_buttons_chunk.setFixedWidth(1)
        sep_buttons_chunk.setFixedHeight(24)
        sep_monitor_search = QtWidgets.QFrame()
        sep_monitor_search.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep_monitor_search.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        sep_monitor_search.setObjectName("toolbarSeparator")
        sep_monitor_search.setFixedWidth(1)
        sep_monitor_search.setFixedHeight(24)

        controls_row.addWidget(self.btn_start_queue)
        controls_row.addWidget(self.btn_pause_queue)
        controls_row.addWidget(self.btn_stop_queue)
        controls_row.addSpacing(10)
        controls_row.addWidget(sep_buttons_chunk)
        controls_row.addSpacing(10)
        controls_row.addWidget(self.chk_enable_chunking)
        controls_row.addWidget(self.spin_chunk_size)
        controls_row.addSpacing(8)
        controls_row.addWidget(self.lbl_retry)
        controls_row.addWidget(self.spin_auto_retry)
        controls_row.addWidget(self.lbl_delay)
        controls_row.addWidget(self.spin_retry_delay)
        controls_row.addSpacing(8)
        controls_row.addWidget(self.chk_disable_husk_mplay)
        controls_row.addWidget(sep_monitor_search)
        controls_row.addSpacing(10)
        controls_row.addStretch(1)
        self.queue_search_edit = QtWidgets.QLineEdit()
        self.queue_search_edit.setPlaceholderText("Search queue...")
        self.queue_search_edit.setClearButtonEnabled(True)
        self.queue_search_edit.setFixedWidth(190)
        self.queue_search_edit.textChanged.connect(self._on_queue_filter_changed)
        controls_row.addWidget(self.queue_search_edit)
        self.queue_status_filter = QtWidgets.QComboBox()
        for label, value in QUEUE_STATUS_FILTER_OPTIONS:
            self.queue_status_filter.addItem(label, value)
        self.queue_status_filter.currentIndexChanged.connect(self._on_queue_filter_changed)
        controls_row.addWidget(self.queue_status_filter)
        self.queue_file_menu_button = QtWidgets.QToolButton()
        self.queue_file_menu_button.setObjectName("queueFileMenuButton")
        self.queue_file_menu_button.setToolButtonStyle(QtCore.Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.queue_file_menu_button.setPopupMode(QtWidgets.QToolButton.ToolButtonPopupMode.InstantPopup)
        self.queue_file_menu_button.setAutoRaise(True)
        self.queue_file_menu_button.setToolTip("Queue File")
        self.queue_file_menu_button.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.queue_file_menu_button.setIconSize(QtCore.QSize(36, 36))
        self.queue_file_menu_button.setFixedSize(40, 40)
        self.queue_file_menu_button.setStyleSheet(
            "QToolButton#queueFileMenuButton { border: none; background: transparent; padding: 2px; }"
            "QToolButton#queueFileMenuButton::menu-indicator { image: none; width: 0px; }"
        )
        self.queue_file_menu_button.setIcon(self._queue_menu_icon())
        self.queue_file_menu = QtWidgets.QMenu(self.queue_file_menu_button)
        self.act_open_queue = self.queue_file_menu.addAction("Open")
        self.act_open_queue.triggered.connect(self._open_queue_file_dialog)
        self.act_save_queue_as = self.queue_file_menu.addAction("Save As")
        self.act_save_queue_as.triggered.connect(self._save_queue_as_dialog)
        self.act_open_queue_folder = self.queue_file_menu.addAction("Open Folder")
        self.act_open_queue_folder.triggered.connect(self._open_current_queue_folder)
        self.queue_file_menu_button.setMenu(self.queue_file_menu)
        controls_row.addWidget(self.queue_file_menu_button)
        controls_host = QtWidgets.QWidget()
        controls_host.setObjectName("queueControlsHost")
        controls_host_layout = QtWidgets.QVBoxLayout(controls_host)
        controls_host_layout.setContentsMargins(8, 8, 8, 8)
        controls_host_layout.setSpacing(0)
        controls_host_layout.addLayout(controls_row)

        self.queue_table = QueueTableWidget()
        self.queue_table_model = QueueTableModel(
            QueueTableModelHooks(
                jobs_provider=lambda: self.jobs,
                is_active_job=self._is_active_job,
                job_phase_display=self._job_phase_display,
                job_usd_status_display=self._job_usd_status_display,
                job_usd_status_tooltip=self._job_usd_status_tooltip,
                job_time_remaining_display=self._job_time_remaining_display,
                job_frame_display=self._job_frame_display,
                job_started_time_display=self._job_started_time_display,
                job_end_time_display=self._job_end_time_display,
                job_total_time_display=self._job_total_time_display,
                queue_progress_split_values=self._queue_progress_split_values,
                edit_job_column=self._apply_queue_cell_edit,
                can_edit_job_column=lambda job, column: can_edit_job_column(
                    job,
                    column=column,
                    is_active_job=self._is_active_job(job),
                    is_locked=self._is_job_path_sync_locked(job),
                ).allowed,
                is_job_path_sync_locked=self._is_job_path_sync_locked,
                row_style_payload=self._queue_row_style_payload,
                theme_icon_path=self._theme_icon_path,
            ),
            self.queue_table,
        )
        self.queue_filter_proxy = QueueFilterProxyModel(self.queue_table)
        self.queue_filter_proxy.setSourceModel(self.queue_table_model)
        self.queue_table.setModel(self.queue_filter_proxy)
        self.queue_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.queue_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.queue_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.queue_table.setAlternatingRowColors(True)
        self.queue_table.setItemDelegate(QueueTableItemDelegate())
        self.queue_table.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.queue_table.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.queue_table.setTextElideMode(QtCore.Qt.TextElideMode.ElideMiddle)
        self.queue_table.verticalScrollBar().setSingleStep(20)
        self.queue_table.horizontalScrollBar().setSingleStep(20)
        self.queue_table.setDragEnabled(True)
        self.queue_table.setAcceptDrops(True)
        self.queue_table.viewport().setAcceptDrops(True)
        self.queue_table.setDropIndicatorShown(False)
        self.queue_table.setDragDropOverwriteMode(False)
        self.queue_table.setDragDropMode(QtWidgets.QAbstractItemView.DragDropMode.DragDrop)
        self.queue_table.setDefaultDropAction(QtCore.Qt.DropAction.MoveAction)
        self.queue_table.verticalHeader().setVisible(False)
        header = self.queue_table.horizontalHeader()
        header.setSectionsMovable(True)
        header.setStretchLastSection(True)
        header.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        for col in range(self.queue_table.columnCount()):
            header.setSectionResizeMode(col, QtWidgets.QHeaderView.ResizeMode.Interactive)
        for logical, width in DEFAULT_QUEUE_COLUMN_WIDTHS.items():
            if 0 <= int(logical) < self.queue_table.columnCount():
                self.queue_table.setColumnWidth(int(logical), int(width))
        self._queue_default_column_widths = {
            logical: int(self.queue_table.columnWidth(logical)) for logical in range(self.queue_table.columnCount())
        }
        self.queue_table.stats_split_after_visual_index = 6
        queue_selection_model = self.queue_table.selectionModel()
        if queue_selection_model is not None:
            queue_selection_model.selectionChanged.connect(lambda *_args: self._on_queue_selection_changed())
        self.queue_table.frame_handling_chosen.connect(self._on_queue_frame_handling_chosen)
        self.queue_table.row_reordered_by_drag.connect(self._on_queue_row_drag_reordered)
        self.queue_table.rows_reordered_by_drag.connect(self._on_queue_rows_drag_reordered)
        header.sectionMoved.connect(self._on_queue_header_section_moved)
        header.sectionResized.connect(self._on_queue_header_section_resized)
        header.customContextMenuRequested.connect(self._show_queue_header_context_menu)
        self._queue_header_valid_order = self._queue_header_visual_order()
        self.queue_table.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.queue_table.customContextMenuRequested.connect(self._show_queue_context_menu)
        if hasattr(self, "queue_tree") and self.queue_tree is not None:
            self.queue_tree.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
            self.queue_tree.customContextMenuRequested.connect(self._show_queue_tree_context_menu)
        self.queue_table.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.queue_table.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        self.queue_table.setLineWidth(0)
        self.queue_table.setMidLineWidth(0)

        queue_table_host = QtWidgets.QWidget()
        queue_table_layout = QtWidgets.QVBoxLayout(queue_table_host)
        queue_table_layout.setContentsMargins(0, 0, 0, 0)
        queue_table_layout.setSpacing(0)
        queue_table_layout.addWidget(self.queue_table)

        queue_left_host = QtWidgets.QWidget()
        queue_left_layout = QtWidgets.QVBoxLayout(queue_left_host)
        queue_left_layout.setContentsMargins(0, 0, 0, 0)
        queue_left_layout.setSpacing(0)
        queue_left_layout.addWidget(controls_host)
        queue_left_layout.addWidget(queue_table_host, 1)

        self.job_properties_panel = JobPropertiesPanel(self)
        self.job_properties_panel.device_mode_changed.connect(self._on_job_properties_device_mode_changed)
        self.job_properties_panel.device_selection_changed.connect(self._on_job_properties_device_selection_changed)
        self.job_properties_panel.render_all_frames_single_process_changed.connect(self._on_job_properties_render_all_frames_single_process_changed)
        self.job_properties_panel.retain_built_usd_changed.connect(self._on_job_properties_retain_built_usd_changed)
        self.job_properties_panel.reuse_retained_usd_changed.connect(self._on_job_properties_reuse_retained_usd_changed)
        self.job_properties_panel.usd_output_directory_mode_changed.connect(self._on_job_properties_usd_output_directory_mode_changed)
        self.job_properties_panel.usd_output_directory_custom_path_changed.connect(self._on_job_properties_usd_output_directory_custom_path_changed)
        self.job_properties_panel.reveal_retained_usd_requested.connect(self._reveal_selected_retained_usd)
        self.job_properties_panel.delete_retained_usd_requested.connect(self._delete_selected_retained_usd)
        self.job_properties_frame = PanelFrame("Job Properties", self.job_properties_panel)
        self.job_properties_frame.setObjectName("jobPropertiesFrame")
        self.job_properties_frame.set_body_margins(0, 0, 0, 0)

        self.queue_properties_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.queue_properties_splitter.addWidget(queue_left_host)
        self.queue_properties_splitter.addWidget(self.job_properties_frame)
        self.queue_properties_splitter.setStretchFactor(0, 5)
        self.queue_properties_splitter.setStretchFactor(1, 2)
        self.queue_properties_splitter.setSizes([940, 320])
        layout.addWidget(self.queue_properties_splitter, 1)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        panel = PanelFrame("Render Queue", box)
        panel.set_body_margins(0, 0, 0, 0)
        return panel

    def _toggle_job_properties_panel(self) -> None:
        focus_widget = QtWidgets.QApplication.focusWidget()
        if isinstance(focus_widget, (QtWidgets.QLineEdit, QtWidgets.QPlainTextEdit, QtWidgets.QTextEdit, QtWidgets.QAbstractSpinBox)):
            return
        if isinstance(focus_widget, QtWidgets.QComboBox) and focus_widget.isEditable():
            return
        splitter = getattr(self, "queue_properties_splitter", None)
        panel = getattr(self, "job_properties_panel", None)
        if splitter is None or panel is None:
            return
        sizes = splitter.sizes()
        if len(sizes) < 2:
            return
        total = max(sum(sizes), 1)
        panel_has_focus = bool(focus_widget is not None and (focus_widget is panel or panel.isAncestorOf(focus_widget)))
        if sizes[1] > 24 and panel_has_focus:
            self._job_properties_last_width = max(int(sizes[1]), 280)
            splitter.setSizes([total, 0])
            if getattr(self, "queue_table", None) is not None:
                self.queue_table.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)
            return
        if sizes[1] <= 24:
            right = max(int(getattr(self, "_job_properties_last_width", 320) or 320), 280)
            right = min(right, max(280, total - 220))
            left = max(1, total - right)
            splitter.setSizes([left, right])
        if getattr(panel, "device_mode_combo", None) is not None and panel.device_mode_combo.isEnabled():
            panel.device_mode_combo.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)
        else:
            panel.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)

    def _build_right_panel(self) -> QtWidgets.QWidget:
        host = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(host)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.right_vertical_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.right_vertical_splitter.addWidget(self._build_queue_panel())
        self.right_vertical_splitter.addWidget(self._build_log_panel())
        self.right_vertical_splitter.setStretchFactor(0, 4)
        self.right_vertical_splitter.setStretchFactor(1, 2)
        layout.addWidget(self.right_vertical_splitter, 1)
        return host

    def _build_log_panel(self) -> QtWidgets.QWidget:
        box = QtWidgets.QGroupBox("Logs")
        layout = QtWidgets.QVBoxLayout(box)

        filter_row = QtWidgets.QHBoxLayout()
        filter_row.addWidget(QtWidgets.QLabel("Filter"))
        self.log_source_filter = QtWidgets.QComboBox()
        self.log_source_filter.addItems(["All", "Stdout", "Stderr"])
        self.log_source_filter.currentIndexChanged.connect(self._refresh_log_view)
        filter_row.addWidget(self.log_source_filter)
        self.log_text_filter = QtWidgets.QLineEdit()
        self.log_text_filter.setPlaceholderText("Contains text...")
        self.log_text_filter.textChanged.connect(self._refresh_log_view)
        filter_row.addWidget(self.log_text_filter, stretch=1)
        self.btn_clear_log_view = QtWidgets.QPushButton("Clear View")
        self.btn_clear_log_view.clicked.connect(self._clear_log_view_only)
        filter_row.addWidget(self.btn_clear_log_view)
        self.btn_copy_diagnostics = QtWidgets.QPushButton("Copy Diagnostics")
        self.btn_copy_diagnostics.clicked.connect(self._copy_diagnostics)
        filter_row.addWidget(self.btn_copy_diagnostics)
        self.btn_open_log_file = QtWidgets.QPushButton("Open Log File")
        self.btn_open_log_file.clicked.connect(self._open_selected_job_log)
        filter_row.addWidget(self.btn_open_log_file)
        layout.addLayout(filter_row)

        self.log_output = QtWidgets.QPlainTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setLineWrapMode(QtWidgets.QPlainTextEdit.LineWrapMode.NoWrap)
        layout.addWidget(self.log_output)
        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        return PanelFrame("Logs", box)

    def _load_hbatch_path(self) -> None:
        configured = str(self.config.get("hbatch_path", "") or "").strip()
        path = configured or discover_hbatch()
        if path:
            self._hbatch_path = path
            if not configured:
                self.config.set("hbatch_path", path)
        else:
            self._hbatch_path = ""

    def _save_hbatch_path(self) -> None:
        self._hbatch_path = self._hbatch_path.strip()
        self.config.set("hbatch_path", self._hbatch_path)
        self._refresh_ui_state()

    def _open_preferences_dialog(self) -> None:
        dlg = PreferencesDialog(
            self._current_hbatch_path(),
            self._current_player_path(),
            dict(self.theme),
            {
                "chunking_enabled": self._default_chunking_enabled(),
                "chunk_size": self._default_chunk_size(),
                "retry_count": self._default_retry_count(),
                "retry_delay": self._default_retry_delay(),
            },
            {
                "chunking": self._experimental_chunking_enabled(),
            },
            {
                "mode": self._default_device_mode().value,
                "selection": self._default_device_selection(),
                "retain_built_usd": self._default_retain_built_usd(),
                "usd_output_directory_mode": self._default_usd_output_directory_mode().value,
                "usd_output_directory_custom_path": self._default_usd_output_directory_custom_path(),
            },
            str(self.config.logs_dir),
            discover_hbatch,
            safe_message,
            self,
        )
        dlg.applied.connect(self._apply_preferences_from_dialog)
        dlg.open_logs_requested.connect(self._open_logs_folder)
        dlg.clear_logs_requested.connect(self._clear_log_files)
        dlg.clear_logs_requested.connect(dlg.refresh_logs_summary)
        dlg.exec()

    def _apply_preferences_from_dialog(self, payload: dict) -> None:
        hbatch_path = str(payload.get("hbatch_path", "")).strip()
        player_path = str(payload.get("player_path", "")).strip()
        theme = payload.get("theme", {})
        runtime_defaults = payload.get("runtime_defaults", {})
        experimental_flags = payload.get("experimental_flags", {})
        device_defaults = payload.get("device_defaults", {})
        self._hbatch_path = hbatch_path
        self._save_hbatch_path()
        self.config.set("player_path", player_path)
        if isinstance(theme, dict):
            self.theme = normalize_theme_colors(theme)
            self.config.save_theme(self.theme)
            self._apply_theme()
        if isinstance(experimental_flags, dict):
            self.config.set("experimental_chunking_enabled", bool(experimental_flags.get("chunking", False)))
        if isinstance(runtime_defaults, dict):
            try:
                chunking_enabled = bool(runtime_defaults.get("chunking_enabled", False))
                chunk_size = max(1, int(runtime_defaults.get("chunk_size", 10)))
                retry_count = max(0, int(runtime_defaults.get("retry_count", 1)))
                retry_delay = max(0, int(runtime_defaults.get("retry_delay", 5)))
            except (TypeError, ValueError):
                chunking_enabled = False
                chunk_size = 10
                retry_count = 1
                retry_delay = 5
            self.config.set("default_chunking_enabled", chunking_enabled)
            self.config.set("default_chunk_size", chunk_size)
            self.config.set("default_retry_count", retry_count)
            self.config.set("default_retry_delay", retry_delay)
            if hasattr(self, "chk_enable_chunking"):
                self.chk_enable_chunking.setChecked(chunking_enabled if self._experimental_chunking_enabled() else False)
            if hasattr(self, "spin_chunk_size"):
                self.spin_chunk_size.setValue(chunk_size)
            if hasattr(self, "spin_auto_retry"):
                self.spin_auto_retry.setValue(retry_count)
            if hasattr(self, "spin_retry_delay"):
                self.spin_retry_delay.setValue(retry_delay)
            self._refresh_ui_state()
        if isinstance(device_defaults, dict):
            mode = DeviceOverrideMode.coerce(device_defaults.get("mode"))
            selection = RenderJob.normalize_device_selection(device_defaults.get("selection", ""))
            retain_built_usd = bool(device_defaults.get("retain_built_usd", False))
            usd_output_directory_mode = UsdOutputDirectoryMode.coerce(device_defaults.get("usd_output_directory_mode"))
            usd_output_directory_custom_path = str(device_defaults.get("usd_output_directory_custom_path", "") or "").strip()
            self.config.set("default_device_mode", mode.value)
            self.config.set("default_device_selection", selection)
            self.config.set("default_retain_built_usd", retain_built_usd)
            self.config.set("default_usd_output_directory_mode", usd_output_directory_mode.value)
            self.config.set("default_usd_output_directory_custom_path", usd_output_directory_custom_path)
            self._update_job_properties_panel()

    def _current_hbatch_path(self) -> str:
        return self._hbatch_path.strip()

    def _current_player_path(self) -> str:
        return str(self.config.get("player_path", "") or "").strip()

    def _current_queue_file_path(self) -> Path:
        return self.queue_file_controller.current_queue_file_path()

    def _set_current_queue_file_path(self, path: Path) -> None:
        self.queue_file_controller.set_current_queue_file_path(path)

    def _update_window_title(self) -> None:
        self.queue_file_controller.update_window_title()

    def _load_queue_from_path(self, path: Path) -> bool:
        try:
            raw = load_queue_payload(path)
            jobs_data = raw.get("jobs", [])
            queue_view = raw.get("queue_view", {})
            active_job_id = str(raw.get("active_job_id", "") or "").strip()
            loaded_jobs: list[RenderJob] = []
            if isinstance(jobs_data, list):
                for item in jobs_data:
                    if not isinstance(item, dict):
                        continue
                    job = self._job_from_persisted_dict(item, active_job_id=active_job_id)
                    if job is not None:
                        loaded_jobs.append(job)
            self.jobs = loaded_jobs
            self._set_current_queue_file_path(path)
            self._reset_queue_view_to_defaults()
            self._apply_queue_view_from_persisted_data(queue_view)
            self._clear_history()
            recovery_summary = build_startup_recovery_summary(self.jobs)
            if self.jobs:
                self._refresh_queue_table(select_row=0)
            else:
                self._refresh_queue_table()
            if recovery_summary is not None:
                self._last_recovery_headline = recovery_summary.headline
                self._append_notification_message(recovery_summary.headline, "warning")
                self._append_log("Stderr", f"[Recovery] {recovery_summary.headline}\n")
                for notice in recovery_summary.notices:
                    self._append_notification_message(notice.message, notice.severity)
                    self._append_log("Stderr", f"[Recovery] {notice.technical_message}\n")
                self._set_status_message(recovery_summary.headline, 6000)
            else:
                self._last_recovery_headline = ""
            return True
        except Exception as exc:
            self._append_log("Stderr", f"[Queue] Failed to load queue: {exc}\n")
            return False

    def _queue_file_dialog_start_dir(self) -> str:
        return self.queue_file_controller.queue_file_dialog_start_dir()

    def _open_queue_file_dialog(self) -> None:
        self.queue_file_controller.open_queue_file_dialog(self)

    def _save_queue_as_dialog(self) -> None:
        self.queue_file_controller.save_queue_as_dialog(self)

    def _open_current_queue_folder(self) -> None:
        self.queue_file_controller.open_current_queue_folder(self)

    def _apply_theme(self) -> None:
        t = normalize_theme_colors(getattr(self, "theme", {}))
        self.theme = t
        icons = ensure_theme_icons(self.config.icons_dir, t)
        self._theme_icons = icons
        style = build_app_stylesheet(t, icons)
        self.setStyleSheet(style)
        panel_gap = int(t.get("panel_gap", 6))
        if hasattr(self, "main_splitter"):
            self.main_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "left_vertical_splitter"):
            self.left_vertical_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "left_column_splitter"):
            self.left_column_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "right_vertical_splitter"):
            self.right_vertical_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "queue_properties_splitter"):
            self.queue_properties_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "queue_table"):
            self.queue_table.selection_line_color = QtGui.QColor(t["selection_line"])
            self.queue_table.selection_row_color = QtGui.QColor(t["selection_row"])
            self.queue_table.selection_row_alt_color = QtGui.QColor(t["selection_row_alt"])
            self.queue_table.selection_overlay_opacity = int(t.get("selection_overlay_opacity", 95))
            self.queue_table.selection_line_enabled = bool(t.get("selection_line_enabled", True))
            self.queue_table.selection_line_thickness = int(t.get("selection_line_thickness", 1))
            self.queue_table.progress_usd_build_color = QtGui.QColor(t["progress_usd_build"])
            self.queue_table.progress_render_color = QtGui.QColor(t["progress_render"])
            self.queue_table.combo_bg_color = QtGui.QColor(t["button_bg"])
            self.queue_table.combo_text_color = QtGui.QColor(t["button_text"])
            self.queue_table.combo_border_color = QtGui.QColor("#555555")
            self.queue_table.setProperty("pathSyncOverlayProgress", self._path_sync_overlay_progress)
            self.queue_table.viewport().update()
            self._refresh_queue_table()
        if hasattr(self, "queue_tree"):
            self.queue_tree.selection_row_color = QtGui.QColor(t["selection_row"])
            self.queue_tree.selection_row_alt_color = QtGui.QColor(t["selection_row_alt"])
            self.queue_tree.selection_overlay_opacity = int(t.get("selection_overlay_opacity", 95))
            self.queue_tree.viewport().update()
        if hasattr(self, "queue_file_menu_button"):
            self.queue_file_menu_button.setIcon(self._queue_menu_icon())
        self._update_bottom_legend_icons()

    def _hbatch_exists(self) -> bool:
        path = self._current_hbatch_path()
        return bool(path) and Path(path).exists()

    def _handle_add_job_requested(self, payload: dict) -> None:
        rop_paths = payload.get("rop_paths")
        if not isinstance(rop_paths, list) or not rop_paths:
            rop_paths = [payload.get("rop_path", "")]

        new_jobs: list[RenderJob] = []
        for rop_path in rop_paths:
            per_payload = dict(payload)
            per_payload["rop_path"] = str(rop_path or "").strip()
            try:
                job = self._build_job_from_payload(per_payload)
            except ValueError as exc:
                safe_message(self, "Invalid Job", str(exc))
                return
            except Exception as exc:
                safe_message(self, "Error", f"Failed to create job: {exc}", traceback.format_exc())
                return

            if hasattr(self, "add_job_panel"):
                strict_hint = self.add_job_panel.rop_strict_frame_range_for_path(job.spec.rop_path)
                job.spec.strict_frame_range = bool(strict_hint)
                allframes_hint = self.add_job_panel.rop_all_frames_single_process_for_path(job.spec.rop_path)
                job.spec.render_all_frames_single_process = bool(allframes_hint)
                output_hint = self.add_job_panel.rop_output_path_for_path(job.spec.rop_path)
                if output_hint:
                    job.view.out_file_sample_path = output_hint
                    job.view.out_path = self._normalize_output_display_path(output_hint)
                rs, re_, rstep = self.add_job_panel.rop_range_info_for_path(job.spec.rop_path)
                if rs is not None and re_ is not None:
                    job.runtime.runtime_start_frame = rs
                    job.runtime.runtime_end_frame = re_
                    job.runtime.runtime_step = rstep
                    job.runtime.rop_default_start_frame = rs
                    job.runtime.rop_default_end_frame = re_
                    job.runtime.rop_default_step = rstep

            if job.spec.frame_range_mode == "use_rop":
                if job.runtime.runtime_start_frame is None or job.runtime.runtime_end_frame is None:
                    try:
                        probe_err = self._probe_and_apply_job_rop_metadata(job, apply_single_process_setting=True)
                    except Exception as exc:
                        probe_err = f"probe_failed: {exc}"
                    if probe_err == "node_not_found":
                        self._mark_job_offline(job, "ROP node not found in HIP file.")
                    elif probe_err:
                        self._append_log("Stderr", f"[Add Job] Could not resolve ROP range for {job.spec.rop_path}: {probe_err}\n")
                        if str(probe_err).startswith("probe_failed:"):
                            self._mark_job_offline(job, str(probe_err))

            if job.spec.frame_range_mode == "override":
                info = self._probe_rop_info(job.spec.hip_path, job.spec.rop_path)
                if info is not None:
                    apply_rop_info_to_job_model(
                        job,
                        info,
                        self._normalize_output_display_path,
                        apply_runtime_range=False,
                        apply_single_process_setting=True,
                    )
                    if hasattr(self, "add_job_panel"):
                        if info.strict_frame_range is not None:
                            self.add_job_panel.set_rop_strict_frame_range_hint(job.spec.rop_path, bool(info.strict_frame_range))
                        if info.all_frames_single_process is not None:
                            self.add_job_panel.set_rop_all_frames_single_process_hint(job.spec.rop_path, bool(info.all_frames_single_process))
                elif hasattr(self, "add_job_panel"):
                    strict_probe = self._probe_rop_strict_frame_range(job.spec.hip_path, job.spec.rop_path)
                    if strict_probe is not None:
                        self.add_job_panel.set_rop_strict_frame_range_hint(job.spec.rop_path, strict_probe)
                        job.spec.strict_frame_range = bool(strict_probe)
                if job.spec.strict_frame_range:
                    return

            new_jobs.append(job)

        if not new_jobs:
            return
        previous_selection = self._selected_job_ids()
        insert_start = len(self.jobs)
        self.jobs.extend(new_jobs)
        for job in new_jobs:
            self.add_job_panel.push_recents_from_job(job.spec.hip_path, job.spec.rop_path)
        self._push_history_command(
            {
                "kind": "insert_jobs",
                "entries": [
                    {"index": insert_start + offset, "job": self._job_to_persisted_dict(job)}
                    for offset, job in enumerate(new_jobs)
                ],
                "undo_select_job_ids": previous_selection,
                "redo_select_job_ids": [job.id for job in new_jobs],
            }
        )
        self._save_queue_state()
        self._refresh_queue_table(select_job_id=new_jobs[-1].id)
        if len(new_jobs) == 1:
            self._set_status_message(f"Added job: {new_jobs[0].display_name()}", 3000)
        else:
            self._set_status_message(f"Added {len(new_jobs)} jobs to queue", 3000)

    def _build_job_from_payload(self, payload: dict) -> RenderJob:
        hip_path = str(payload.get("hip_path", "")).strip()
        rop_path = str(payload.get("rop_path", "")).strip()
        name = str(payload.get("name", "")).strip()
        mode = str(payload.get("frame_range_mode", "use_rop"))
        start_raw = payload.get("start_frame", 1)
        end_raw = payload.get("end_frame", 1)
        step_raw = payload.get("step", 1)
        start_frame = int(start_raw) if start_raw is not None else None
        end_frame = int(end_raw) if end_raw is not None else None
        step = int(step_raw) if step_raw is not None else None

        if not hip_path:
            raise ValueError("HIP file path is required.")
        if not Path(hip_path).exists():
            raise ValueError(f"HIP file does not exist:\n{hip_path}")
        if not rop_path:
            raise ValueError("ROP path is required.")
        if not rop_path.startswith("/"):
            raise ValueError("ROP path should look like /out/my_rop.")
        if mode not in {"use_rop", "override"}:
            raise ValueError("Invalid frame range mode.")
        if mode == "override":
            if start_frame is None or end_frame is None:
                raise ValueError("Override frame range requires start and end frame values.")
            if step <= 0:
                raise ValueError("Step must be >= 1.")
            if end_frame < start_frame:
                raise ValueError("End frame must be >= start frame.")
        else:
            start_frame = end_frame = step = None

        job = RenderJob(
            hip_path=str(Path(hip_path)),
            rop_path=rop_path,
            frame_range_mode=mode,
            start_frame=start_frame,
            end_frame=end_frame,
            step=step,
            name=name,
            device_override_mode=DeviceOverrideMode.DEFAULT,
            device_selection="",
            render_all_frames_single_process=False,
            retain_built_usd=self._default_retain_built_usd(),
            reuse_retained_usd=False,
            usd_output_directory_mode=self._default_usd_output_directory_mode(),
            usd_output_directory_custom_path=self._default_usd_output_directory_custom_path(),
        )
        job.runtime.log_file_path = str(self.config.new_job_log_path(job.display_name()))
        job.view.phase_text = ""
        job.view.progress_text = "-"
        job.view.percent_text = "-"
        return job

    def _probe_rop_info(self, hip_path: str, rop_path: str) -> RopInfo | None:
        return self.scan_coordinator.probe_rop_info(hip_path, rop_path)

    def _probe_and_apply_job_rop_metadata(self, job: RenderJob, *, apply_single_process_setting: bool = False) -> str | None:
        return self.scan_coordinator.probe_and_apply_job_rop_metadata(
            job,
            apply_single_process_setting=apply_single_process_setting,
        )

    def _probe_rop_strict_frame_range(self, hip_path: str, rop_path: str) -> bool | None:
        return self.scan_coordinator.probe_rop_strict_frame_range(hip_path, rop_path)

    def _scan_rop_info_for_hip(self, hip_path: str) -> dict[str, RopInfo]:
        return self.scan_coordinator.scan_rop_info_for_hip(hip_path)

    def _project_houdini_scripts_dir(self) -> Path:
        return project_houdini_scripts_dir_model(__file__, HOUDINI_SCRIPTS_DIR_NAME)

    def _required_houdini_script_filenames(self) -> list[str]:
        return required_houdini_script_filenames_model()

    def _validate_houdini_script_files(self) -> bool:
        scripts_dir = self._project_houdini_scripts_dir()
        missing = validate_houdini_script_files_model(scripts_dir)
        if not missing:
            return True
        if not self._houdini_scripts_missing_warned:
            self._houdini_scripts_missing_warned = True
            safe_message(
                self,
                "Missing Houdini Scripts",
                f"Required helper scripts are missing from:\n{scripts_dir}",
                "Missing files:\n" + "\n".join(missing),
            )
        return False

    def _load_houdini_script_text(self, filename: str) -> str:
        return load_houdini_script_text_model(self._project_houdini_scripts_dir(), filename)

    def _build_render_preflight_script(self, job: RenderJob, disable_husk_mplay: bool, hook_paths: dict[str, str]) -> str:
        return build_render_preflight_script_model(
            scripts_dir=self._project_houdini_scripts_dir(),
            rop_path=job.spec.rop_path,
            disable_husk_mplay=disable_husk_mplay,
            hook_paths=hook_paths,
        )

    def _retained_usd_helper_path(self) -> Path:
        config = getattr(self, "config", None)
        if config is not None:
            hook_script_path = getattr(config, "hook_script_path", None)
            if callable(hook_script_path):
                try:
                    return Path(hook_script_path("hsrm_retained_usd_paths"))
                except Exception as exc:
                    _log_suppressed_exception("MainWindow._retained_usd_helper_path", exc)
        return get_appdata_dir() / "hooks" / "hsrm_retained_usd_paths.py"

    def _build_render_environment(self, job: RenderJob) -> dict[str, str]:
        mode = self._effective_device_mode_for_job(job)
        selection = self._effective_device_selection_for_job(job)
        self._sync_retained_usd_file_state(job)
        selected_tokens = [part.strip().lower() for part in str(selection or "").split(",") if part.strip()]
        cpu_selected = "cpu" in selected_tokens
        selected_gpu_ids = [part for part in selected_tokens if part.isdigit()]
        all_gpu_ids = [str(device.get("id", "") or "") for device in self._available_render_devices() if str(device.get("id", "") or "").isdigit()]
        single_process_render = self._single_process_render_enabled_for_job(job)
        retain_usd_enabled = bool(job.spec.retain_built_usd) and single_process_render
        env: dict[str, str] = {
            "HSRM_DEVICE_MODE": mode.value,
            "HSRM_DEVICE_SELECTION": selection,
            "HSRM_DEVICE_INCLUDE_CPU": "1" if cpu_selected else "0",
            "HSRM_RENDER_ALL_FRAMES_SINGLE_PROCESS": "1" if single_process_render else "0",
            "HSRM_RETAIN_USD_ENABLED": "1" if retain_usd_enabled else "0",
            "HSRM_RETAIN_USD_HELPER_PATH": str(self._retained_usd_helper_path()),
        }
        if retain_usd_enabled:
            planned_build_range = self._current_retained_usd_build_range(job)
            output_path = str(job.runtime.retained_usd_path or "").strip()
            configured_output_dir = self._configured_retained_usd_folder_preview(job)
            invalid_reason = self._retained_usd_invalid_reason(job)
            should_delete_existing = bool(
                output_path
                and (
                    not bool(job.spec.reuse_retained_usd)
                    or bool(invalid_reason)
                )
            )
            if should_delete_existing:
                self._delete_retained_usd_folder_for_job(job)
                self._sync_retained_usd_file_state(job)
                if planned_build_range is not None:
                    job.runtime.retained_usd_build_start_frame = int(planned_build_range[0])
                    job.runtime.retained_usd_build_end_frame = int(planned_build_range[1])
                    job.runtime.retained_usd_build_step = int(planned_build_range[2])
                output_path = str(job.runtime.retained_usd_path or "").strip()
            reuse_existing = bool(
                job.spec.reuse_retained_usd
                and output_path
                and job.runtime.retained_usd_reusable
                and Path(output_path).exists()
                and not invalid_reason
            )
            job.runtime.retained_usd_metadata_pending_write = bool(not reuse_existing)
            if reuse_existing and output_path:
                env["HSRM_RETAIN_USD_OUTPUT_PATH"] = output_path
            elif configured_output_dir:
                env["HSRM_RETAIN_USD_OUTPUT_DIR"] = configured_output_dir
            env["HSRM_REUSE_EXISTING_USD"] = "1" if reuse_existing else "0"
            if reuse_existing:
                stale_reason = self._retained_usd_stale_reason(job)
                if stale_reason:
                    self._append_log("Stderr", f"[RetainUSD] {stale_reason}\n")
                    self._append_notification_message(stale_reason, "warning")
            elif invalid_reason:
                self._append_log("Stderr", f"[RetainUSD] {invalid_reason}\n")
                self._append_notification_message(invalid_reason, "warning")
        if mode is DeviceOverrideMode.CPU:
            env["HOUDINI_OCL_DEVICETYPE"] = "CPU"
            env["CUDA_VISIBLE_DEVICES"] = "-1"
        elif mode is DeviceOverrideMode.ALL_GPUS:
            env["HOUDINI_OCL_DEVICETYPE"] = "GPU"
            if all_gpu_ids:
                env["CUDA_VISIBLE_DEVICES"] = ",".join(all_gpu_ids)
        elif mode is DeviceOverrideMode.SPECIFIC_GPUS:
            if selected_gpu_ids:
                env["HOUDINI_OCL_DEVICETYPE"] = "GPU"
                env["CUDA_VISIBLE_DEVICES"] = ",".join(selected_gpu_ids)
            elif cpu_selected:
                env["HOUDINI_OCL_DEVICETYPE"] = "CPU"
                env["CUDA_VISIBLE_DEVICES"] = "-1"
        return env

    def _queue_proxy_model(self) -> QueueFilterProxyModel | None:
        proxy = getattr(self, "queue_filter_proxy", None)
        return proxy if isinstance(proxy, QueueFilterProxyModel) else None

    def _queue_source_row_from_view_row(self, view_row: int) -> int:
        proxy = self._queue_proxy_model()
        if proxy is None:
            return int(view_row)
        proxy_index = proxy.index(int(view_row), 0)
        if not proxy_index.isValid():
            return -1
        source_index = proxy.mapToSource(proxy_index)
        return int(source_index.row()) if source_index.isValid() else -1

    def _queue_view_index_from_source_row(self, source_row: int, column: int = 0) -> QtCore.QModelIndex:
        proxy = self._queue_proxy_model()
        source_index = self.queue_table_model.index(int(source_row), int(column))
        if proxy is None:
            return source_index
        return proxy.mapFromSource(source_index)

    def _queue_source_rows_from_view_rows(self, view_rows: list[int]) -> list[int]:
        return source_rows_from_view_rows_model(
            view_rows,
            source_row_for_view_row=self._queue_source_row_from_view_row,
            job_count=len(self.jobs),
        )

    def _selected_row(self) -> int:
        model = self.queue_table.selectionModel()
        rows = model.selectedRows() if model is not None else []
        return selected_row_from_view_rows_model(
            [idx.row() for idx in rows if idx.isValid()],
            source_row_for_view_row=self._queue_source_row_from_view_row,
        )

    def _selected_rows(self) -> list[int]:
        model = self.queue_table.selectionModel()
        if model is None:
            return []
        return self._queue_source_rows_from_view_rows([idx.row() for idx in model.selectedRows() if idx.isValid()])

    def _selected_jobs(self) -> list[RenderJob]:
        return [self.jobs[r] for r in self._selected_rows() if 0 <= r < len(self.jobs)]

    def _selected_job_ids(self) -> list[str]:
        return [job.id for job in self._selected_jobs()]

    @staticmethod
    def _mixed_value(values: list[Any]) -> tuple[bool, Any]:
        return mixed_value_model(values)

    def _effective_device_mode_for_job(self, job: RenderJob) -> DeviceOverrideMode:
        return job.effective_device_mode(self._default_device_mode())

    def _effective_device_selection_for_job(self, job: RenderJob) -> str:
        return job.effective_device_selection(self._default_device_selection())

    def _effective_device_summary_for_job(self, job: RenderJob) -> str:
        return job.device_summary(
            self._default_device_mode(),
            self._default_device_selection(),
        )

    @staticmethod
    def _job_file_name(job: RenderJob) -> str:
        path = str(job.spec.hip_path or "").strip()
        if not path:
            return "-"
        return Path(path).name or path

    @staticmethod
    def _job_rop_name(job: RenderJob) -> str:
        rop_path = str(job.spec.rop_path or "").strip().rstrip("/")
        if not rop_path:
            return "-"
        return rop_path.split("/")[-1] or rop_path

    @staticmethod
    def _safe_usd_folder_name(name: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name or "").strip()).strip("_")
        return cleaned or "rop"

    def _effective_usd_output_directory_mode_for_job(self, job: RenderJob) -> UsdOutputDirectoryMode:
        return UsdOutputDirectoryMode.coerce(job.spec.usd_output_directory_mode)

    def _effective_usd_output_directory_custom_path_for_job(self, job: RenderJob) -> str:
        return str(job.spec.usd_output_directory_custom_path or "").strip()

    @staticmethod
    def _single_process_render_enabled_for_job(job: RenderJob) -> bool:
        return bool(job.spec.render_all_frames_single_process)

    def _configured_retained_usd_folder_preview(self, job: RenderJob) -> str:
        mode = self._effective_usd_output_directory_mode_for_job(job)
        if mode is UsdOutputDirectoryMode.DEFAULT_TEMP:
            return ""
        if mode is UsdOutputDirectoryMode.PROJECT_PATH:
            hip_path = str(job.spec.hip_path or "").strip()
            if not hip_path:
                return ""
            hip_name = Path(hip_path).stem or "hip"
            base_dir = Path(hip_path).parent / "usd_renders" / self._safe_usd_folder_name(hip_name)
        else:
            custom_path = self._effective_usd_output_directory_custom_path_for_job(job)
            if not custom_path:
                return ""
            base_dir = Path(custom_path)
        rop_name = self._safe_usd_folder_name(self._job_rop_name(job))
        return str(base_dir / f"{rop_name}_$RENDERID")

    def _device_option_states_for_jobs(
        self,
        jobs: list[RenderJob],
        *,
        show_custom_devices: bool,
        editable: bool,
    ) -> list[dict[str, Any]]:
        if not show_custom_devices:
            return []
        devices = self._available_render_devices()
        if not devices:
            return []
        selected_sets = []
        for job in jobs:
            normalized = RenderJob.normalize_device_selection(job.spec.device_selection)
            selected_sets.append(set(part for part in normalized.split(",") if part))
        option_states: list[dict[str, Any]] = []
        for device in devices:
            device_id = str(device.get("id", "") or "")
            selected_count = sum(1 for selected in selected_sets if device_id in selected)
            if selected_count <= 0:
                check_state = QtCore.Qt.CheckState.Unchecked
            elif selected_count >= len(selected_sets):
                check_state = QtCore.Qt.CheckState.Checked
            else:
                check_state = QtCore.Qt.CheckState.PartiallyChecked
            option_states.append(
                {
                    "id": device_id,
                    "name": str(device.get("name", "") or device_id),
                    "check_state": int(getattr(check_state, "value", check_state)),
                    "enabled": editable,
                }
            )
        return option_states

    def _selected_retained_usd_paths(self) -> list[Path]:
        return selected_retained_usd_paths_model(
            self._selected_jobs(),
            is_absolute_path=self._is_absolute_retained_usd_path,
        )

    @staticmethod
    def _is_absolute_retained_usd_path(path_text: str) -> bool:
        return is_absolute_retained_usd_path_model(path_text)

    @staticmethod
    def _clear_retained_usd_runtime(job: RenderJob) -> None:
        clear_retained_usd_runtime_model(job)

    def _job_properties_panel_default_state(self) -> dict[str, Any]:
        default_usd_output_mode = self._default_usd_output_directory_mode()
        return default_job_properties_panel_state_model(
            default_usd_output_mode=default_usd_output_mode.value,
            default_usd_output_custom_path=self._default_usd_output_directory_custom_path(),
            retained_usd_defaults=retained_usd_panel_default_fields_model(),
        )

    def _single_job_retained_usd_panel_state(self, job: RenderJob) -> dict[str, Any]:
        return single_job_retained_usd_panel_state_model(
            job,
            sync_file_state=self._sync_retained_usd_file_state,
            load_metadata=self._load_retained_usd_metadata,
            build_info_text=self._retained_usd_build_info,
            built_at_text=self._retained_usd_built_at_text,
            is_absolute_path=self._is_absolute_retained_usd_path,
            configured_folder_preview=self._configured_retained_usd_folder_preview,
            hip_stale_reason=self._retained_usd_hip_stale_reason,
            stale_reason=self._retained_usd_stale_reason,
            invalid_reason=self._retained_usd_invalid_reason,
            status_text=self._retained_usd_status_text,
        )

    def _sync_retained_usd_file_state(self, job: RenderJob) -> None:
        sync_retained_usd_file_state_model(
            job,
            invalid_reason_for_job=self._retained_usd_invalid_reason,
            should_write_metadata_now=self._should_write_retained_usd_metadata_now,
            write_metadata=self._write_retained_usd_metadata,
        )

    @staticmethod
    def _should_write_retained_usd_metadata_now(job: RenderJob) -> bool:
        return should_write_retained_usd_metadata_now_model(job)

    def _retained_usd_metadata_path(self, retained_usd_path: Path) -> Path:
        return retained_usd_metadata_path_model(retained_usd_path)

    def _current_retained_usd_build_range(self, job: RenderJob) -> tuple[int, int, int] | None:
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        if resolved is None:
            return None
        start, end, step = resolved
        return int(start), int(end), int(step)

    def _current_retained_usd_reuse_range(self, job: RenderJob) -> tuple[int, int, int] | None:
        if (
            job.runtime.status == JobStatus.RUNNING
            and job.runtime.chunk_total_runtime > 0
            and
            job.runtime.chunk_start_frame_runtime is not None
            and job.runtime.chunk_end_frame_runtime is not None
            and (job.runtime.chunk_step_runtime or 0) > 0
        ):
            return (
                int(job.runtime.chunk_start_frame_runtime),
                int(job.runtime.chunk_end_frame_runtime),
                int(job.runtime.chunk_step_runtime),
            )
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        if resolved is None:
            return None
        start, end, step = resolved
        return int(start), int(end), int(step)

    def _load_retained_usd_metadata(self, retained_usd_path: Path) -> dict[str, Any] | None:
        metadata_path = self._retained_usd_metadata_path(retained_usd_path)
        if not metadata_path.exists():
            return None
        data = read_json_file(metadata_path)
        if not isinstance(data, dict):
            return None
        return data

    def _write_retained_usd_metadata(self, job: RenderJob, retained_usd_path: Path) -> None:
        write_retained_usd_metadata_model(
            job,
            retained_usd_path,
            metadata_path_for=self._retained_usd_metadata_path,
            append_log=self._append_log,
            now_fn=datetime.now,
        )

    @staticmethod
    def _retained_usd_build_info(metadata: dict[str, Any] | None) -> tuple[str, str]:
        return retained_usd_build_info_model(metadata)

    @staticmethod
    def _retained_usd_built_at_text(metadata: dict[str, Any] | None) -> str:
        return retained_usd_built_at_text_model(metadata)

    def _retained_usd_hip_stale_reason(self, job: RenderJob, metadata: dict[str, Any] | None) -> str:
        return retained_usd_hip_stale_reason_model(str(job.spec.hip_path or ""), metadata)

    def _retained_usd_status_text(self, job: RenderJob, metadata: dict[str, Any] | None) -> str:
        return retained_usd_status_text_model(
            single_process_render_enabled=self._single_process_render_enabled_for_job(job),
            retain_built_usd=bool(job.spec.retain_built_usd),
            reuse_retained_usd=bool(job.spec.reuse_retained_usd),
            retained_path=str(job.runtime.retained_usd_path or "").strip(),
            retained_usd_exists=bool(job.runtime.retained_usd_exists),
            metadata=metadata,
            invalid_reason=self._retained_usd_invalid_reason(job),
        )

    def _retained_usd_invalid_reason(self, job: RenderJob) -> str:
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        metadata = self._load_retained_usd_metadata(Path(retained_path)) if retained_path else None
        current_range = self._current_retained_usd_reuse_range(job)
        return retained_usd_invalid_reason_model(
            single_process_render_enabled=self._single_process_render_enabled_for_job(job),
            retain_built_usd=bool(job.spec.retain_built_usd),
            reuse_retained_usd=bool(job.spec.reuse_retained_usd),
            retained_path=retained_path,
            metadata=metadata,
            current_range=current_range,
        )

    def _delete_retained_usd_folder_for_job(self, job: RenderJob) -> bool:
        return delete_retained_usd_folder_for_job_model(
            job,
            is_absolute_path=self._is_absolute_retained_usd_path,
            clear_runtime=self._clear_retained_usd_runtime,
            append_log=self._append_log,
        )

    def _retained_usd_stale_reason(self, job: RenderJob) -> str:
        self._sync_retained_usd_file_state(job)
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        if not retained_path:
            return ""
        metadata = self._load_retained_usd_metadata(Path(retained_path))
        stale_reason = self._retained_usd_hip_stale_reason(job, metadata)
        if stale_reason:
            return stale_reason
        invalid_reason = self._retained_usd_invalid_reason(job)
        if invalid_reason:
            return invalid_reason
        return ""

    def _apply_job_property_edit(
        self,
        *,
        property_name: str,
        apply_fn,
        target_jobs: list[RenderJob],
    ) -> None:
        if not target_jobs:
            return
        editable = [job for job in target_jobs if can_edit_job(job, is_active_job=self._is_active_job(job), is_locked=self._is_job_path_sync_locked(job)).allowed]
        if not editable:
            return
        target_ids = [job.id for job in editable]
        before_states = self._job_states_for_ids(target_ids)
        changed = False
        for job in editable:
            changed = bool(apply_fn(job) or changed)
        if not changed:
            self._update_job_properties_panel()
            return
        after_states = self._job_states_for_ids(target_ids)
        self._push_history_command(
            {
                "kind": "update_jobs",
                "before": before_states,
                "after": after_states,
                "undo_select_job_ids": self._selected_job_ids(),
                "redo_select_job_ids": self._selected_job_ids(),
            }
        )
        self._save_and_refresh_queue(select_job_ids=self._selected_job_ids())

    def _update_job_properties_panel(self) -> None:
        panel = getattr(self, "job_properties_panel", None)
        if panel is None:
            return
        unchecked_state = int(getattr(QtCore.Qt.CheckState.Unchecked, "value", QtCore.Qt.CheckState.Unchecked))
        checked_state = int(getattr(QtCore.Qt.CheckState.Checked, "value", QtCore.Qt.CheckState.Checked))
        partial_state = int(getattr(QtCore.Qt.CheckState.PartiallyChecked, "value", QtCore.Qt.CheckState.PartiallyChecked))
        selected_jobs = self._selected_jobs()
        if not selected_jobs:
            panel.set_state(self._job_properties_panel_default_state())
            return

        selected_count = len(selected_jobs)
        mixed_name, first_name = self._mixed_value([job.display_name() for job in selected_jobs])
        mixed_file, first_file = self._mixed_value([self._job_file_name(job) for job in selected_jobs])
        mixed_rop, first_rop = self._mixed_value([self._job_rop_name(job) for job in selected_jobs])
        mixed_device_mode, first_device_mode = self._mixed_value([job.spec.device_override_mode.value for job in selected_jobs])
        mixed_single_process, first_single_process = self._mixed_value([bool(job.spec.render_all_frames_single_process) for job in selected_jobs])
        mixed_retain, first_retain = self._mixed_value([bool(job.spec.retain_built_usd) for job in selected_jobs])
        mixed_reuse, first_reuse = self._mixed_value([bool(job.spec.reuse_retained_usd) for job in selected_jobs])
        mixed_usd_output_mode, first_usd_output_mode = self._mixed_value([job.spec.usd_output_directory_mode.value for job in selected_jobs])
        mixed_usd_output_custom_path, first_usd_output_custom_path = self._mixed_value([str(job.spec.usd_output_directory_custom_path or "") for job in selected_jobs])
        retained_usd_state: dict[str, Any]
        retained_paths: list[Path] = []
        if selected_count == 1:
            retained_usd_state = self._single_job_retained_usd_panel_state(selected_jobs[0])
        else:
            retained_paths = self._selected_retained_usd_paths()
            retained_usd_state = multi_job_retained_usd_panel_state_model(retained_paths)

        editable = all(
            can_edit_job(job, is_active_job=self._is_active_job(job), is_locked=self._is_job_path_sync_locked(job)).allowed
            for job in selected_jobs
        )
        current_device_mode = DeviceOverrideMode.coerce(first_device_mode)
        show_custom_devices = current_device_mode is DeviceOverrideMode.SPECIFIC_GPUS and not mixed_device_mode
        can_delete = can_delete_retained_usd_model(
            selected_count=selected_count,
            retained_state_can_open=bool(retained_usd_state["can_open"]),
            retained_paths_present=bool(retained_paths),
            has_active_or_locked_job=any(self._is_active_job(job) or self._is_job_path_sync_locked(job) for job in selected_jobs),
        )

        panel.set_state(
            build_job_properties_panel_state_model(
                mixed_name=mixed_name,
                first_name=first_name,
                mixed_file=mixed_file,
                first_file=first_file,
                mixed_rop=mixed_rop,
                first_rop=first_rop,
                editable=editable,
                mixed_device_mode=mixed_device_mode,
                first_device_mode=first_device_mode,
                show_custom_devices=show_custom_devices,
                device_options=self._device_option_states_for_jobs(
                    selected_jobs,
                    show_custom_devices=show_custom_devices,
                    editable=editable,
                ),
                mixed_single_process=mixed_single_process,
                first_single_process=bool(first_single_process),
                mixed_retain=mixed_retain,
                first_retain=bool(first_retain),
                mixed_reuse=mixed_reuse,
                first_reuse=bool(first_reuse),
                mixed_usd_output_mode=mixed_usd_output_mode,
                first_usd_output_mode=first_usd_output_mode,
                mixed_usd_output_custom_path=mixed_usd_output_custom_path,
                first_usd_output_custom_path=str(first_usd_output_custom_path or ""),
                retained_usd_state=retained_usd_state,
                can_delete=can_delete,
                unchecked_state=unchecked_state,
                checked_state=checked_state,
                partial_state=partial_state,
                default_device_mode=DeviceOverrideMode.DEFAULT.value,
                default_usd_output_mode=UsdOutputDirectoryMode.DEFAULT_TEMP.value,
            )
        )

    def _apply_job_property_edit_spec(self, spec: JobPropertyEditSpec) -> None:
        property_name, apply_fn = spec
        self._apply_job_property_edit(
            property_name=property_name,
            apply_fn=apply_fn,
            target_jobs=self._selected_jobs(),
        )

    def _on_job_properties_device_mode_changed(self, value: str) -> None:
        self._apply_job_property_edit_spec(device_mode_edit_spec_model(value))

    def _on_job_properties_device_selection_changed(self, value: str) -> None:
        self._apply_job_property_edit_spec(device_selection_edit_spec_model(value))

    def _on_job_properties_retain_built_usd_changed(self, checked: bool) -> None:
        self._apply_job_property_edit_spec(retain_built_usd_edit_spec_model(checked))

    def _on_job_properties_render_all_frames_single_process_changed(self, checked: bool) -> None:
        self._apply_job_property_edit_spec(single_process_render_edit_spec_model(checked))

    def _on_job_properties_reuse_retained_usd_changed(self, checked: bool) -> None:
        self._apply_job_property_edit_spec(reuse_retained_usd_edit_spec_model(checked))

    def _on_job_properties_usd_output_directory_mode_changed(self, value: str) -> None:
        self._apply_job_property_edit_spec(usd_output_directory_mode_edit_spec_model(value))

    def _on_job_properties_usd_output_directory_custom_path_changed(self, value: str) -> None:
        self._apply_job_property_edit_spec(usd_output_directory_custom_path_edit_spec_model(value))

    def _reveal_selected_retained_usd(self) -> None:
        paths = self._selected_retained_usd_paths()
        if not paths:
            safe_message(self, "Retained USD", "No retained USD file is available for the current selection.")
            return
        target_folder = first_retained_usd_folder_model(paths)
        if target_folder is None:
            safe_message(self, "Retained USD", "No retained USD file is available for the current selection.")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(target_folder)))

    def _delete_selected_retained_usd(self) -> None:
        selected_jobs = self._selected_jobs()
        paths = self._selected_retained_usd_paths()
        if not selected_jobs or not paths:
            safe_message(self, "Retained USD", "No retained USD file is available for the current selection.")
            return
        if any(self._is_active_job(job) or self._is_job_path_sync_locked(job) for job in selected_jobs):
            safe_message(self, "Retained USD", "Wait for the current job or path update to finish.")
            return
        delete_result = delete_retained_usd_directories_model(paths)
        if delete_result.error is not None and delete_result.error_dir is not None:
            safe_message(
                self,
                "Retained USD",
                f"Failed to delete retained USD folder:\n{delete_result.error_dir}",
                str(delete_result.error),
            )
            return
        if not delete_result.deleted_any:
            return
        target_dirs = set(delete_result.target_dirs)
        target_ids = [job.id for job in selected_jobs]
        before_states = self._job_states_for_ids(target_ids)
        clear_deleted_retained_usd_runtime_model(
            selected_jobs,
            target_dirs,
            clear_runtime=self._clear_retained_usd_runtime,
        )
        after_states = self._job_states_for_ids(target_ids)
        self._push_history_command(
            {
                "kind": "update_jobs",
                "before": before_states,
                "after": after_states,
                "undo_select_job_ids": target_ids,
                "redo_select_job_ids": target_ids,
            }
        )
        self._save_and_refresh_queue(select_job_ids=target_ids)

    def _refresh_queue_preserve_selection(self) -> None:
        selected_ids = self._selected_job_ids()
        selected = self._selected_job()
        self._refresh_queue_table(
            select_job_ids=selected_ids or None,
            select_job_id=None if selected_ids else (selected.id if selected is not None else None),
        )

    def _queue_refresh_should_defer(self) -> bool:
        focus = QtWidgets.QApplication.focusWidget()
        focus_in_queue = bool(focus is self.queue_table or (focus is not None and self.queue_table.isAncestorOf(focus)))
        focus_in_add_panel = bool(
            hasattr(self, "add_job_panel")
            and self.add_job_panel is not None
            and focus is not None
            and self.add_job_panel.isAncestorOf(focus)
        )
        return should_defer_queue_refresh_model(
            focus=focus,
            queue_is_editing=self.queue_table.state() == QtWidgets.QAbstractItemView.State.EditingState,
            focus_in_queue=focus_in_queue,
            focus_in_add_panel=focus_in_add_panel,
            queue_editable_types=(
                QtWidgets.QLineEdit,
                QtWidgets.QAbstractSpinBox,
                QtWidgets.QComboBox,
                QtWidgets.QPlainTextEdit,
                QtWidgets.QTextEdit,
            ),
            add_panel_editable_types=(
                QtWidgets.QLineEdit,
                QtWidgets.QAbstractSpinBox,
                QtWidgets.QComboBox,
                QtWidgets.QPlainTextEdit,
                QtWidgets.QTextEdit,
                QtWidgets.QListWidget,
            ),
        )

    def _on_queue_filter_changed(self) -> None:
        proxy = self._queue_proxy_model()
        if proxy is None:
            return
        selected_ids = self._selected_job_ids()
        proxy.set_search_text(self.queue_search_edit.text() if hasattr(self, "queue_search_edit") else "")
        current_value = ""
        if hasattr(self, "queue_status_filter") and self.queue_status_filter is not None:
            current_value = str(self.queue_status_filter.currentData() or "")
        proxy.set_status_filter(current_value)
        proxy.set_enabled_only(False)
        self._refresh_queue_table(select_job_ids=selected_ids or None)
        self._refresh_ui_state()

    def _flush_pending_queue_refresh(self) -> None:
        args, should_reschedule = next_pending_refresh_action_model(
            self._pending_queue_refresh_args,
            should_defer=self._queue_refresh_should_defer(),
        )
        if should_reschedule:
            self._pending_queue_refresh_timer.start(200)
            return
        if args is None:
            return
        self._pending_queue_refresh_args = None
        self._refresh_queue_table(**args)

    def _is_job_runnable(self, job: RenderJob | None) -> bool:
        return is_job_runnable(job, is_locked=self._is_job_path_sync_locked(job))

    def _reset_job_state(self, job: RenderJob) -> None:
        reset_job_state_model(job)

    def _prepare_duplicate_job(self, source: RenderJob) -> RenderJob | None:
        clone = self._job_from_persisted_dict(self._job_to_persisted_dict(source))
        if clone is None:
            return None
        clone.spec.id = uuid4().hex
        clone.runtime.status = JobStatus.QUEUED
        clone.runtime.started_at = None
        clone.runtime.finished_at = None
        clone.runtime.exit_code = None
        clone.runtime.error_summary = ""
        clone.runtime.offline_detected_reason = ""
        self._clear_retained_usd_runtime(clone)
        clone.view.progress_text = "-"
        clone.view.percent_text = "-"
        clone.view.usd_build_percent = None
        clone.view.last_frame_seen = None
        clone.view.build_pass_completed = False
        clone.view.phase_text = ""
        clone.view.prev_frame_time_text = "-"
        clone.view.avg_frame_time_text = "-"
        clone.view.est_job_time_text = "-"
        clone.view.render_frame_started_at = {}
        clone.view.render_frame_durations_sec = []
        clone.view.render_completed_frames = set()
        clone.runtime.offline_previous_status = None
        clone.runtime.resume_start_frame_runtime = None
        clone.runtime.resume_end_frame_runtime = None
        clone.runtime.resume_step_runtime = None
        clone.runtime.resume_completed_baseline_count = 0
        clone.runtime.chunk_start_frame_runtime = None
        clone.runtime.chunk_end_frame_runtime = None
        clone.runtime.chunk_step_runtime = None
        clone.runtime.chunk_index_runtime = 0
        clone.runtime.chunk_total_runtime = 0
        clone.runtime.chunk_attempt_runtime = 0
        clone.runtime.chunk_retry_count_runtime = 0
        clone.runtime.chunk_ranges_runtime = []
        clone.runtime.chunk_retry_total_failures_runtime = 0
        clone.runtime.log_file_path = str(self.config.new_job_log_path(clone.display_name()))
        return clone

    def _reset_jobs_to_queued(self, target_jobs: list[RenderJob]) -> list[str]:
        changed_ids: list[str] = []
        for job in target_jobs:
            if self._is_active_job(job):
                continue
            before = (
                job.runtime.status,
                job.runtime.error_summary,
                job.interrupted_reason,
                job.runtime.started_at,
                job.runtime.finished_at,
                job.exit_code,
            )
            self._reset_job_state(job)
            after = (
                job.runtime.status,
                job.runtime.error_summary,
                job.interrupted_reason,
                job.runtime.started_at,
                job.runtime.finished_at,
                job.exit_code,
            )
            if before != after:
                changed_ids.append(job.id)
        return changed_ids

    @staticmethod
    def _clear_job_resume_runtime_state(job: RenderJob) -> None:
        clear_job_resume_runtime_state_model(job)

    def _save_and_refresh_queue(
        self,
        *,
        select_job_id: str | None = None,
        select_job_ids: list[str] | None = None,
        select_row: int | None = None,
    ) -> None:
        self._save_queue_state()
        self._refresh_queue_table(select_row=select_row, select_job_id=select_job_id, select_job_ids=select_job_ids)

    def _mark_job_offline(self, job: RenderJob, reason: str | None = None) -> None:
        mark_job_offline_model(job, reason)

    def _restore_job_online_status(self, job: RenderJob) -> None:
        restore_job_online_status_model(job)

    def _selection_ids_for_refresh(self, fallback_job_ids: list[str] | None = None) -> list[str] | None:
        selected_ids = self._selected_job_ids()
        if selected_ids:
            return selected_ids
        if fallback_job_ids:
            return [job_id for job_id in fallback_job_ids if job_id]
        return None

    def _defer_refresh_queue_tree_view(self) -> None:
        QtCore.QTimer.singleShot(0, self._refresh_queue_tree_view)

    def _defer_save_and_refresh_queue(
        self,
        select_job_ids: list[str] | None = None,
        *,
        block_interaction: bool = False,
        status_text: str | None = None,
    ) -> None:
        ids = list(select_job_ids or [])
        if block_interaction:
            self._begin_interaction_lock(status_text or "Applying change...")

        def _finish(ids: list[str]) -> None:
            try:
                self._save_and_refresh_queue(
                    select_job_ids=self._selection_ids_for_refresh(ids)
                )
            finally:
                if block_interaction:
                    self._end_interaction_lock()

        QtCore.QTimer.singleShot(
            0,
            lambda ids=ids: _finish(ids),
        )

    def _tree_context_target_jobs(self, index: QtCore.QModelIndex) -> list[RenderJob]:
        if not index.isValid():
            return []
        hip_path = str(index.data(TREE_HIP_ROLE) or "").strip()
        rop_path = str(index.data(TREE_ROP_ROLE) or "").strip()
        kind = str(index.data(TREE_KIND_ROLE) or "").strip().lower()
        if not hip_path:
            return []
        if kind == "rop" and rop_path:
            return [job for job in self.jobs if job.spec.hip_path == hip_path and job.spec.rop_path == rop_path]
        return [job for job in self.jobs if job.spec.hip_path == hip_path]

    def _show_queue_tree_context_menu(self, pos: QtCore.QPoint) -> None:
        if not hasattr(self, "queue_tree") or self.queue_tree is None:
            return
        index = self.queue_tree.indexAt(pos)
        if not index.isValid():
            return
        self.queue_tree.setCurrentIndex(index)
        target_jobs = self._tree_context_target_jobs(index)
        if not target_jobs:
            return

        menu = QtWidgets.QMenu(self)
        act_select = menu.addAction("Select")
        act_remove = menu.addAction("Remove")
        any_locked = any(self._is_job_path_sync_locked(job) for job in target_jobs)
        act_remove.setEnabled((not any_locked) and any(not self._is_active_job(job) for job in target_jobs))

        chosen = menu.exec(self.queue_tree.viewport().mapToGlobal(pos))
        if chosen is None:
            return
        if chosen == act_select:
            self._refresh_queue_table(select_job_ids=[job.id for job in target_jobs])
            return
        if chosen == act_remove:
            if any_locked:
                safe_message(self, "Please Wait", "Wait for the current path update to finish.")
                return
            removable = [job for job in target_jobs if not self._is_active_job(job)]
            if not removable:
                safe_message(self, "Cannot Remove", "Cannot remove the active running job.")
                return
            removed_entries = [
                {"index": idx, "job": self._job_to_persisted_dict(job)}
                for idx, job in enumerate(self.jobs)
                if job.id in {target.id for target in removable}
            ]
            removable_ids = {job.id for job in removable}
            running_blocked = any(self._is_active_job(job) for job in target_jobs)
            self.jobs = [job for job in self.jobs if job.id not in removable_ids]
            self._push_history_command(
                {
                    "kind": "remove_jobs",
                    "entries": removed_entries,
                    "undo_select_job_ids": [entry["job"]["id"] for entry in removed_entries],
                    "redo_select_job_ids": [],
                }
            )
            self._save_and_refresh_queue()
            if running_blocked:
                safe_message(
                    self,
                    "Some Jobs Not Removed",
                    "The active running job cannot be removed. Other matching jobs were removed.",
                )

    def _propagate_hip_path_change(self, old_hip: str, new_hip: str) -> list[str]:
        return propagate_hip_path_change_model(
            self.jobs,
            old_hip=old_hip,
            new_hip=new_hip,
            running_status=JobStatus.RUNNING,
            probe_rop_info=self._probe_rop_info,
            mark_job_offline=self._mark_job_offline,
            restore_job_online_status=self._restore_job_online_status,
            normalize_output_display_path=self._normalize_output_display_path,
        )

    def _apply_hip_path_change_immediately(self, old_hip: str, new_hip: str) -> list[str]:
        return apply_hip_path_change_immediately_model(
            self.jobs,
            old_hip=old_hip,
            new_hip=new_hip,
            running_status=JobStatus.RUNNING,
        )

    def _affected_job_ids_for_hip_path_change(self, old_hip: str) -> list[str]:
        return affected_job_ids_for_hip_path_change_model(self.jobs, old_hip)

    def _propagate_rop_path_change(self, hip_path: str, old_rop: str, new_rop: str) -> list[str]:
        return propagate_rop_path_change_model(
            self.jobs,
            hip_path=hip_path,
            old_rop=old_rop,
            new_rop=new_rop,
            running_status=JobStatus.RUNNING,
            probe_rop_info=self._probe_rop_info,
            mark_job_offline=self._mark_job_offline,
            restore_job_online_status=self._restore_job_online_status,
            normalize_output_display_path=self._normalize_output_display_path,
        )

    def _apply_rop_path_change_immediately(self, hip_path: str, old_rop: str, new_rop: str) -> list[str]:
        return apply_rop_path_change_immediately_model(
            self.jobs,
            hip_path=hip_path,
            old_rop=old_rop,
            new_rop=new_rop,
            running_status=JobStatus.RUNNING,
        )

    def _affected_job_ids_for_rop_path_change(self, hip_path: str, old_rop: str) -> list[str]:
        return affected_job_ids_for_rop_path_change_model(self.jobs, hip_path, old_rop)

    def _refresh_jobs_from_rop_metadata(
        self,
        target_jobs: list[RenderJob],
        *,
        reset_override_to_rop: bool = False,
    ) -> list[str]:
        return refresh_jobs_from_rop_metadata_model(
            target_jobs,
            running_status=JobStatus.RUNNING,
            scan_rop_info_for_hip=self._scan_rop_info_for_hip,
            probe_rop_info=self._probe_rop_info,
            mark_job_offline=self._mark_job_offline,
            restore_job_online_status=self._restore_job_online_status,
            clear_job_resume_runtime_state=self._clear_job_resume_runtime_state,
            normalize_output_display_path=self._normalize_output_display_path,
            reset_override_to_rop=reset_override_to_rop,
        )

    def _sync_jobs_after_path_change(self, target_jobs: list[RenderJob]) -> list[str]:
        sync_jobs_after_path_change_model(
            target_jobs,
            probe_rop_info=self._probe_rop_info,
            mark_job_offline=self._mark_job_offline,
            restore_job_online_status=self._restore_job_online_status,
            normalize_output_display_path=self._normalize_output_display_path,
        )
        return [job.id for job in target_jobs]

    def _defer_finalize_path_change(
        self,
        *,
        changed_ids: list[str],
        before_states: list[dict[str, Any]],
        undo_select_job_ids: list[str],
        redo_select_job_ids: list[str],
        status_text: str,
    ) -> None:
        defer_finalize_path_change_model(
            changed_ids=changed_ids,
            before_states=before_states,
            undo_select_job_ids=undo_select_job_ids,
            redo_select_job_ids=redo_select_job_ids,
            status_text=status_text,
            begin_path_sync_lock=self._begin_path_sync_lock,
            enqueue_path_sync_task=self._enqueue_path_sync_task,
        )

    def _defer_reload_jobs_from_file(
        self,
        target_jobs: list[RenderJob],
        *,
        reset_override_to_rop: bool,
        status_text: str,
        notification_label: str,
    ) -> None:
        defer_reload_jobs_from_file_model(
            target_jobs,
            reset_override_to_rop=reset_override_to_rop,
            status_text=status_text,
            notification_label=notification_label,
            job_states_for_ids=self._job_states_for_ids,
            begin_path_sync_lock=self._begin_path_sync_lock,
            enqueue_path_sync_task=self._enqueue_path_sync_task,
        )

    def _on_queue_tree_item_changed(self, item: QtGui.QStandardItem) -> None:
        if getattr(self, "_suppress_tree_item_changed", False):
            return
        if item is None:
            return
        try:
            kind = str(item.data(TREE_KIND_ROLE) or "").strip()
            text = item.text().strip()
            old_hip = str(item.data(TREE_HIP_ROLE) or "").strip()
            old_rop = str(item.data(TREE_ROP_ROLE) or "").strip()
            if kind == "hip":
                target_ids = self._affected_job_ids_for_hip_path_change(old_hip)
                before_states = self._job_states_for_ids(target_ids)
                if not text:
                    self._defer_refresh_queue_tree_view()
                    return
                try:
                    text = validate_queue_path_value_model(1, text)
                except ValueError as exc:
                    safe_message(self, "Invalid Path", str(exc))
                    self._defer_refresh_queue_tree_view()
                    return
                if text == old_hip:
                    if item.text() != old_hip:
                        self._suppress_tree_item_changed = True
                        try:
                            item.setText(old_hip)
                        finally:
                            self._suppress_tree_item_changed = False
                    return
                changed_ids = self._apply_hip_path_change_immediately(old_hip, text)
                self._defer_finalize_path_change(
                    changed_ids=changed_ids,
                    before_states=before_states,
                    undo_select_job_ids=target_ids,
                    redo_select_job_ids=changed_ids,
                    status_text="Updating HIP path...",
                )
                return
            if kind == "rop":
                target_ids = self._affected_job_ids_for_rop_path_change(old_hip, old_rop)
                before_states = self._job_states_for_ids(target_ids)
                if not text:
                    self._defer_refresh_queue_tree_view()
                    return
                try:
                    text = validate_queue_path_value_model(2, text)
                except ValueError as exc:
                    safe_message(self, "Invalid Path", str(exc))
                    self._defer_refresh_queue_tree_view()
                    return
                if text == old_rop:
                    if item.text() != old_rop:
                        self._suppress_tree_item_changed = True
                        try:
                            item.setText(old_rop)
                        finally:
                            self._suppress_tree_item_changed = False
                    return
                changed_ids = self._apply_rop_path_change_immediately(old_hip, old_rop, text)
                self._defer_finalize_path_change(
                    changed_ids=changed_ids,
                    before_states=before_states,
                    undo_select_job_ids=target_ids,
                    redo_select_job_ids=changed_ids,
                    status_text="Updating ROP path...",
                )
                return
        except Exception as exc:
            safe_message(self, "Tree Edit Error", f"Failed to apply tree edit: {exc}", traceback.format_exc())
            self._defer_refresh_queue_tree_view()

    def _job_can_reset_cached_cell(self, job: RenderJob, col: int) -> bool:
        cached_start = job.runtime.rop_default_start_frame
        cached_end = job.runtime.rop_default_end_frame
        cached_step = job.runtime.rop_default_step
        if job.runtime.status == JobStatus.RUNNING:
            return False
        if col == 3:
            return (
                cached_start is not None
                and cached_end is not None
                and not job.spec.strict_frame_range
            )
        if col == 4:
            return (cached_step not in (None, 0)) and (not job.spec.strict_frame_range)
        return False

    @staticmethod
    def _normalize_job_override_mode_against_cached(job: RenderJob) -> None:
        if job.spec.frame_range_mode != "override":
            return
        cached_start = job.runtime.rop_default_start_frame
        cached_end = job.runtime.rop_default_end_frame
        cached_step = job.runtime.rop_default_step
        if (
            cached_start is None
            or cached_end is None
            or cached_step in (None, 0)
            or job.spec.start_frame is None
            or job.spec.end_frame is None
            or job.spec.step is None
        ):
            return
        try:
            matches_range = (
                int(job.spec.start_frame) == int(float(cached_start))
                and int(job.spec.end_frame) == int(float(cached_end))
            )
            matches_step = int(job.spec.step) == int(float(cached_step))
        except (TypeError, ValueError):
            return
        if matches_range and matches_step:
            job.spec.frame_range_mode = "use_rop"
            job.spec.start_frame = None
            job.spec.end_frame = None
            job.spec.step = None

    @staticmethod
    def _queue_edit_frame_text_for_job(job: RenderJob) -> str:
        return job.frame_range_display()

    @staticmethod
    def _queue_edit_step_text_for_job(job: RenderJob) -> str:
        return job.step_display()

    def _reset_cached_cell_for_jobs(self, col: int, target_jobs: list[RenderJob]) -> bool:
        changed = False
        for target in target_jobs:
            if not self._job_can_reset_cached_cell(target, col):
                continue
            cached_start = target.runtime.rop_default_start_frame
            cached_end = target.runtime.rop_default_end_frame
            cached_step = target.runtime.rop_default_step
            if col == 3:
                try:
                    rs = int(float(cached_start))
                    re = int(float(cached_end))
                except (TypeError, ValueError):
                    continue
                if target.frame_range_mode == "use_rop":
                    continue
                target.start_frame = rs
                target.end_frame = re
                changed = True
            elif col == 4:
                try:
                    rstep = int(float(cached_step))
                except (TypeError, ValueError):
                    continue
                if target.frame_range_mode == "use_rop":
                    continue
                if int(target.step or 1) == rstep:
                    continue
                target.step = rstep
                changed = True
            self._normalize_job_override_mode_against_cached(target)
        return changed

    def _show_queue_context_menu(self, pos: QtCore.QPoint) -> None:
        idx = self.queue_table.indexAt(pos)
        if idx.isValid():
            sm = self.queue_table.selectionModel()
            if sm is not None and not sm.isRowSelected(idx.row(), QtCore.QModelIndex()):
                self.queue_table.selectRow(idx.row())
        job = self._selected_job()
        if job is None:
            return
        selected_jobs = self._selected_jobs()
        target_jobs = selected_jobs or [job]
        any_active = any(j.status == JobStatus.RUNNING and self.current_job_id == j.id for j in target_jobs)

        menu = QtWidgets.QMenu(self)
        out_folder = self._output_folder_from_value(job.view.out_path)
        has_finished_jobs = any(j.runtime.status in {JobStatus.DONE, JobStatus.FAILED} for j in self.jobs)
        any_locked = any(self._is_job_path_sync_locked(j) for j in target_jobs)
        act_toggle = menu.addAction("Disable" if job.spec.enabled else "Enable")
        act_toggle.setEnabled((not any_active) and (not any_locked))
        act_reset = menu.addAction("Reset State")
        act_reset.setEnabled((not any_active) and (not any_locked))
        act_reset_cell_cached = None
        if idx.isValid() and idx.column() in {3, 4}:
            menu.addSeparator()
            act_reset_cell_cached = menu.addAction("Reset Value")
            act_reset_cell_cached.setEnabled((not any_locked) and any(self._job_can_reset_cached_cell(t, idx.column()) for t in target_jobs))
        act_reload_from_rop = menu.addAction("Reload Values from File")
        reload_decision = can_reload_jobs_from_file(
            target_jobs=target_jobs,
            is_active_job_fn=self._is_active_job,
            hbatch_exists=self._hbatch_exists(),
            is_locked_job_fn=self._is_job_path_sync_locked,
        )
        act_reload_from_rop.setEnabled(reload_decision.allowed)
        menu.addSeparator()
        duplicate_decision = can_duplicate_jobs(
            target_jobs,
            is_active_job_fn=self._is_active_job,
            scan_in_progress=self._scan_in_progress(),
            is_locked_job_fn=self._is_job_path_sync_locked,
        )
        act_duplicate = menu.addAction("Duplicate")
        act_duplicate.setEnabled(duplicate_decision.allowed)
        act_remove = menu.addAction("Remove")
        act_remove.setEnabled((not any_active) and (not any_locked))
        act_clear_finished = menu.addAction("Clear Finished")
        act_clear_finished.setEnabled(has_finished_jobs)
        menu.addSeparator()
        preview_path = self._job_preview_path(job)
        preview_player_path = self._current_player_path()
        preview_decision = can_preview_job(
            preview_path_exists=bool(preview_path),
            player_path_set=bool(preview_player_path),
            player_exists=bool(preview_player_path and Path(preview_player_path).exists()),
        )
        act_preview = menu.addAction("Preview")
        act_preview.setEnabled(preview_decision.allowed)
        act_open_folder = menu.addAction("Open Folder")
        open_folder_decision = can_open_output_folder(folder_exists=bool(out_folder and out_folder.exists()))
        act_open_folder.setEnabled(open_folder_decision.allowed)

        chosen = menu.exec(self.queue_table.viewport().mapToGlobal(pos))
        if chosen is None:
            return
        if chosen == act_preview:
            self._preview_job(job)
        elif chosen == act_open_folder and out_folder is not None:
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(out_folder)))
        elif chosen == act_toggle:
            new_enabled = not job.spec.enabled
            target_ids = [j.id for j in target_jobs]
            before_states = self._job_states_for_ids(target_ids)
            for target in target_jobs:
                if not self._is_active_job(target):
                    target.enabled = new_enabled
            after_states = self._job_states_for_ids(target_ids)
            self._push_history_command(
                {
                    "kind": "update_jobs",
                    "before": before_states,
                    "after": after_states,
                    "undo_select_job_ids": target_ids,
                    "redo_select_job_ids": target_ids,
                }
            )
            self._save_and_refresh_queue(select_job_ids=[j.id for j in target_jobs])
        elif chosen == act_reset:
            target_ids = [j.id for j in target_jobs]
            before_states = self._job_states_for_ids(target_ids)
            for target in target_jobs:
                if not self._is_active_job(target):
                    self._reset_job_state(target)
            after_states = self._job_states_for_ids(target_ids)
            self._push_history_command(
                {
                    "kind": "update_jobs",
                    "before": before_states,
                    "after": after_states,
                    "undo_select_job_ids": target_ids,
                    "redo_select_job_ids": target_ids,
                }
            )
            self._save_and_refresh_queue(select_job_ids=[j.id for j in target_jobs])
        elif act_reset_cell_cached is not None and chosen == act_reset_cell_cached:
            target_ids = [j.id for j in target_jobs]
            before_states = self._job_states_for_ids(target_ids)
            if idx.isValid() and idx.column() in {3, 4} and self._reset_cached_cell_for_jobs(idx.column(), target_jobs):
                after_states = self._job_states_for_ids(target_ids)
                self._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_ids,
                        "redo_select_job_ids": target_ids,
                    }
                )
                self._save_and_refresh_queue(select_job_ids=[j.id for j in target_jobs])
        elif chosen == act_reload_from_rop:
            if not reload_decision.allowed:
                safe_message(self, "Reload From File", reload_decision.reason)
                return
            self._defer_reload_jobs_from_file(
                target_jobs,
                reset_override_to_rop=True,
                status_text="Reloading values from file...",
                notification_label="Reload Values from File",
            )
        elif chosen == act_duplicate:
            self._duplicate_selected_jobs()
        elif chosen == act_remove:
            self._remove_selected_job()
        elif chosen == act_clear_finished:
            self._clear_finished_jobs()

    def _resolve_job_range_for_execution(
        self, job: RenderJob, *, mutate_job: bool = True
    ) -> tuple[int, int, int] | None:
        if job.spec.frame_range_mode == "override":
            if job.spec.start_frame is None or job.spec.end_frame is None:
                return None
            return int(job.spec.start_frame), int(job.spec.end_frame), int(job.spec.step or 1)
        if (
            job.runtime.runtime_start_frame is not None
            and job.runtime.runtime_end_frame is not None
            and job.runtime.runtime_step not in (None, 0)
        ):
            return (
                int(job.runtime.runtime_start_frame),
                int(job.runtime.runtime_end_frame),
                int(job.runtime.runtime_step),
            )
        if mutate_job:
            try:
                err = self._probe_and_apply_job_rop_metadata(job)
            except Exception as exc:
                self._append_log("Stderr", f"[ROP Probe] Unexpected error for {job.spec.rop_path}: {exc}\n")
                self._mark_job_offline(job, f"Failed to resolve ROP metadata: {exc}")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if err == "node_not_found":
                self._mark_job_offline(job, "ROP node not found in HIP file.")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if err:
                if str(err).startswith("probe_failed:"):
                    self._mark_job_offline(job, str(err))
                    self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if (
                job.runtime.runtime_start_frame is not None
                and job.runtime.runtime_end_frame is not None
                and job.runtime.runtime_step not in (None, 0)
            ):
                return (
                    int(job.runtime.runtime_start_frame),
                    int(job.runtime.runtime_end_frame),
                    int(job.runtime.runtime_step),
                )
        else:
            try:
                info = self._probe_rop_info(job.spec.hip_path, job.spec.rop_path)
            except Exception as exc:
                self._append_log("Stderr", f"[ROP Probe] Unexpected error for {job.spec.rop_path}: {exc}\n")
                self._mark_job_offline(job, f"Failed to resolve ROP metadata: {exc}")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if info is None:
                return None
            if info.error == "node_not_found":
                self._mark_job_offline(job, "ROP node not found in HIP file.")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if (
                info.runtime_start_frame is not None
                and info.runtime_end_frame is not None
                and info.runtime_step not in (None, 0)
            ):
                return (
                    int(info.runtime_start_frame),
                    int(info.runtime_end_frame),
                    int(info.runtime_step),
                )
        return None

    @staticmethod
    def _frame_sequence_path_for_frame(sample_path: str, frame: int) -> Path | None:
        return frame_sequence_path_for_frame_model(sample_path, frame)

    @staticmethod
    def _normalize_output_display_path(path_text: str) -> str:
        return normalize_output_display_path_model(path_text)

    @staticmethod
    def _output_folder_from_value(path_text: str) -> Path | None:
        return output_folder_from_value_model(path_text)

    def _compute_resume_from_output(
        self,
        job: RenderJob,
        *,
        interactive: bool = True,
    ) -> tuple[int, int, int, int] | None:
        sample_file_path = (job.view.out_file_sample_path or "").strip()
        out_path = (job.view.out_path or "").strip()
        probe_path = sample_file_path or out_path
        strict_decision = validate_resume_from_output_inputs(strict_frame_range=job.spec.strict_frame_range)
        if not strict_decision.valid:
            if interactive and strict_decision.title:
                safe_message(self, strict_decision.title, strict_decision.message)
            return None
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        resolved_decision = validate_resolved_frame_range_for_resume(resolved, offline=job.runtime.status == JobStatus.OFFLINE)
        if not resolved_decision.valid:
            if interactive and resolved_decision.title:
                safe_message(self, resolved_decision.title, resolved_decision.message)
            return None
        assert resolved is not None
        start_frame, end_frame, step = resolved

        # If we only have a folder (or an unpatterned path), refresh metadata from the ROP first.
        # This often gives us a resolved sample filename path without needing to start the render.
        needs_pattern_refresh = (
            (not probe_path)
            or (not sample_file_path)
            or str(probe_path).lower() == "ip"
            or self._frame_sequence_path_for_frame(sample_file_path or probe_path, start_frame) is None
        )
        if needs_pattern_refresh and Path(job.spec.hip_path).exists() and self._hbatch_exists():
            info = self._probe_rop_info(job.spec.hip_path, job.spec.rop_path)
            if info is None:
                info = None
            if info is not None and info.error == "node_not_found":
                self._mark_job_offline(job, "ROP node not found in HIP file.")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if info is not None:
                # For resume pre-check, refresh only output/strict metadata; do not mutate
                # the displayed frame range/step values in the queue.
                apply_rop_info_to_job_model(
                    job,
                    info,
                    self._normalize_output_display_path,
                    apply_runtime_range=False,
                )
            refreshed_sample = (job.view.out_file_sample_path or "").strip()
            if refreshed_sample:
                probe_path = refreshed_sample
                sample_file_path = refreshed_sample

        probe_decision = validate_resume_probe_path(
            probe_path=probe_path,
            pattern_resolved=self._frame_sequence_path_for_frame(probe_path, start_frame) is not None,
        )
        if not probe_decision.valid:
            if interactive and probe_decision.title:
                safe_message(self, probe_decision.title, probe_decision.message)
            return None

        def _exists_nonempty(path: Path) -> bool:
            try:
                exists = path.exists()
                return bool(exists and path.stat().st_size > 0)
            except OSError:
                return False

        scan = first_missing_frame_and_contiguous_done_model(
            start_frame=start_frame,
            end_frame=end_frame,
            step=step,
            path_for_frame=lambda frame: self._frame_sequence_path_for_frame(probe_path, frame),
            exists_nonempty=_exists_nonempty,
        )
        if scan is None:
            return None
        first_missing, contiguous_done, total = scan

        if contiguous_done >= total:
            if interactive:
                safe_message(
                    self,
                    "Resume From Output",
                    "All frames in the job range appear to exist already.",
                )
                return None
            # Sentinel tuple for "already complete" in non-interactive skip-existing mode.
            return end_frame + step, end_frame, step, contiguous_done

        if first_missing is None:
            first_missing = start_frame
        return first_missing, end_frame, step, contiguous_done

    def _compute_missing_ranges_from_output(
        self,
        job: RenderJob,
        *,
        interactive: bool = True,
    ) -> tuple[list[tuple[int, int, int]], int] | None:
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        resolved_decision = validate_render_missing_inputs(resolved, offline=job.runtime.status == JobStatus.OFFLINE)
        if not resolved_decision.valid:
            if interactive and resolved_decision.title:
                safe_message(self, resolved_decision.title, resolved_decision.message)
            return None
        assert resolved is not None
        start_frame, end_frame, step = resolved

        sample_file_path = (job.view.out_file_sample_path or "").strip()
        out_path = (job.view.out_path or "").strip()
        probe_path = sample_file_path or out_path
        needs_pattern_refresh = (
            (not probe_path)
            or (not sample_file_path)
            or str(probe_path).lower() == "ip"
            or self._frame_sequence_path_for_frame(sample_file_path or probe_path, start_frame) is None
        )
        if needs_pattern_refresh and Path(job.spec.hip_path).exists() and self._hbatch_exists():
            info = self._probe_rop_info(job.spec.hip_path, job.spec.rop_path)
            if info is not None and info.error == "node_not_found":
                self._mark_job_offline(job, "ROP node not found in HIP file.")
                self._save_and_refresh_queue(select_job_id=job.id)
                return None
            if info is not None:
                apply_rop_info_to_job_model(
                    job,
                    info,
                    self._normalize_output_display_path,
                    apply_runtime_range=False,
                )
            refreshed_sample = (job.view.out_file_sample_path or "").strip()
            if refreshed_sample:
                probe_path = refreshed_sample

        probe_decision = validate_render_missing_probe_path(
            probe_path=probe_path,
            pattern_resolved=self._frame_sequence_path_for_frame(probe_path, start_frame) is not None,
        )
        if not probe_decision.valid:
            if interactive and probe_decision.title:
                safe_message(self, probe_decision.title, probe_decision.message)
            return None

        def _exists_nonempty(path: Path) -> bool:
            try:
                exists = path.exists()
                return bool(exists and path.stat().st_size > 0)
            except OSError:
                return False

        scan = missing_frame_runs_and_existing_count_model(
            start_frame=start_frame,
            end_frame=end_frame,
            step=step,
            path_for_frame=lambda frame: self._frame_sequence_path_for_frame(probe_path, frame),
            exists_nonempty=_exists_nonempty,
        )
        if scan is None:
            return None
        return scan

    def _resume_job_from_output(self, job: RenderJob) -> None:
        decision = can_resume_job_from_output(
            job,
            render_job_active=self._render_job_active(),
            queue_active=self.queue_active,
            hip_exists=Path(job.spec.hip_path).exists(),
            hbatch_exists=self._hbatch_exists(),
        )
        if not decision.allowed:
            if decision.reason == "HIP file not found.":
                self._mark_job_offline(job, "HIP file not found.")
                self._save_queue_state()
                self._refresh_queue_table(select_job_id=job.id)
            elif decision.reason:
                safe_message(self, "Resume From Output", decision.reason)
            return
        resume_info = self._compute_resume_from_output(job)
        if resume_info is None:
            return
        resume_start, resume_end, resume_step, baseline_done = resume_info
        job.runtime.resume_start_frame_runtime = int(resume_start)
        job.runtime.resume_end_frame_runtime = int(resume_end)
        job.runtime.resume_step_runtime = int(resume_step)
        job.runtime.resume_completed_baseline_count = int(max(0, baseline_done))
        self._append_log(
            "Stdout",
            f"[Resume] {job.display_name()} from output: start={resume_start}, end={resume_end}, step={resume_step}, existing={baseline_done}\n",
        )
        self._refresh_queue_table(select_job_id=job.id)
        self._start_queue()

    def _job_to_persisted_dict(self, job: RenderJob) -> dict[str, Any]:
        return job_to_persisted_dict_model(job)

    def _queue_view_to_persisted_dict(self) -> dict[str, Any]:
        return queue_view_to_persisted_dict_model(self.queue_table)

    def _job_from_persisted_dict(self, data: dict[str, Any], *, active_job_id: str | None = None) -> RenderJob | None:
        return job_from_persisted_dict_model(data, active_job_id=active_job_id)

    def _job_states_for_ids(self, job_ids: list[str]) -> list[dict[str, Any]]:
        return job_states_for_ids_model(self.jobs, job_ids)

    def _remove_jobs_by_ids(self, job_ids: list[str]) -> None:
        self.jobs = remove_jobs_by_ids_model(self.jobs, job_ids)

    def _insert_jobs_from_entries(self, entries: list[dict[str, Any]]) -> None:
        self.jobs = insert_jobs_from_entries_model(self.jobs, entries)

    def _apply_job_states(self, states: list[dict[str, Any]]) -> None:
        self.jobs = apply_job_states_model(self.jobs, states)

    def _apply_job_order(self, ordered_ids: list[str]) -> None:
        self.jobs = apply_job_order_model(self.jobs, ordered_ids)

    def _push_history_command(self, command: dict[str, Any]) -> None:
        if not should_push_history_command_model(history_applying=self._history_applying, command=command):
            return
        self._undo_stack.append(command)
        self._undo_stack = bounded_undo_stack_model(self._undo_stack, max_size=100)
        self._redo_stack.clear()

    def _clear_history(self) -> None:
        self._undo_stack.clear()
        self._redo_stack.clear()

    def _history_command_targets_active_job(self, command: dict[str, Any]) -> bool:
        active_job_id = str(self.current_job_id or "")
        return history_command_targets_job_model(command, active_job_id=active_job_id)

    def _apply_history_command(self, command: dict[str, Any], *, undo: bool) -> None:
        select_ids: list[str] = []
        self._history_applying = True
        try:
            select_ids = apply_history_command_model(
                command,
                undo=undo,
                remove_jobs_by_ids=self._remove_jobs_by_ids,
                insert_jobs_from_entries=self._insert_jobs_from_entries,
                apply_job_states=self._apply_job_states,
                apply_job_order=self._apply_job_order,
            )
        finally:
            self._history_applying = False
        self._save_queue_state()
        self._refresh_queue_table(select_job_ids=select_ids or None)

    def _undo_queue_edit(self) -> None:
        command = pop_history_for_shortcut_model(
            self._undo_stack,
            scan_in_progress=self._scan_in_progress(),
            command_targets_active=self._history_command_targets_active_job,
        )
        if command is None:
            return
        self._apply_history_command(command, undo=True)
        self._redo_stack.append(command)
        self._set_status_message("Undo", 1500)

    def _redo_queue_edit(self) -> None:
        command = pop_history_for_shortcut_model(
            self._redo_stack,
            scan_in_progress=self._scan_in_progress(),
            command_targets_active=self._history_command_targets_active_job,
        )
        if command is None:
            return
        self._apply_history_command(command, undo=False)
        self._undo_stack.append(command)
        self._set_status_message("Redo", 1500)

    def _save_queue_state(self, path: Path | None = None) -> bool:
        try:
            target_path = path or self._current_queue_file_path()
            target_path.parent.mkdir(parents=True, exist_ok=True)
            save_queue_payload(
                target_path,
                jobs=self.jobs,
                queue_view=self._queue_view_to_persisted_dict(),
                active_job_id=self.current_job_id,
            )
            self._set_current_queue_file_path(target_path)
            return True
        except (OSError, TypeError, ValueError) as exc:
            self._append_log("Stderr", f"[Queue] Failed to save queue: {exc}\n")
            return False

    def _write_queue_snapshot(self, reason: str, *, max_files: int = 5) -> bool:
        try:
            write_queue_snapshot_model(
                base_dir=self.config.base_dir,
                reason=reason,
                jobs=self.jobs,
                queue_view=self._queue_view_to_persisted_dict(),
                active_job_id=self.current_job_id,
                save_queue_payload_fn=save_queue_payload,
                max_files=max_files,
            )
            return True
        except (OSError, TypeError, ValueError) as exc:
            self._append_log("Stderr", f"[Queue] Failed to write queue snapshot ({reason}): {exc}\n")
            return False

    def _build_queue_run_summary(self, started_job_ids: set[str]) -> tuple[str, str] | None:
        return build_queue_run_summary_model(self.jobs, started_job_ids)

    def _load_persisted_queue(self) -> None:
        path = self._current_queue_file_path()
        if not path.exists():
            return
        self._load_queue_from_path(path)

    def _selected_job(self) -> RenderJob | None:
        row = self._selected_row()
        if 0 <= row < len(self.jobs):
            return self.jobs[row]
        return None

    def _remove_selected_job(self) -> None:
        rows = self._selected_rows()
        if not rows:
            return
        previous_selection = self._selected_job_ids()
        selected_jobs = [self.jobs[r] for r in rows if 0 <= r < len(self.jobs)]
        removable_rows = [r for r in rows if not self._is_active_job(self.jobs[r])]
        remove_decision = can_remove_jobs(
            selected_jobs,
            is_active_job_fn=self._is_active_job,
            is_locked_job_fn=self._is_job_path_sync_locked,
        )
        if not remove_decision.allowed:
            safe_message(self, "Cannot Remove", remove_decision.reason)
            return
        if not removable_rows:
            safe_message(self, "Cannot Remove", f"{remove_decision.reason} Use Stop to cancel it.")
            return
        removed_entries = [
            {"index": r, "job": self._job_to_persisted_dict(self.jobs[r])}
            for r in removable_rows
        ]
        for r in reversed(removable_rows):
            del self.jobs[r]
        self._push_history_command(
            {
                "kind": "remove_jobs",
                "entries": removed_entries,
                "undo_select_job_ids": [entry["job"]["id"] for entry in removed_entries],
                "redo_select_job_ids": previous_selection,
            }
        )
        if len(removable_rows) != len(rows):
            safe_message(
                self,
                "Some Jobs Not Removed",
                "The active running job cannot be removed. Other selected jobs were removed.",
            )
        remaining_job_ids = {j.id for j in self.jobs}
        remaining_selected_ids = [
            j.id for j in selected_jobs if self._is_active_job(j) and j.id in remaining_job_ids
        ]
        next_row = min(removable_rows[0], len(self.jobs) - 1) if self.jobs else None
        self._save_and_refresh_queue(
            select_job_ids=remaining_selected_ids or None,
            select_row=next_row,
        )

    def _duplicate_selected_jobs(self) -> None:
        rows = self._selected_rows()
        if not rows:
            return
        source_jobs = [self.jobs[row] for row in rows if 0 <= row < len(self.jobs)]
        duplicate_decision = can_duplicate_jobs(
            source_jobs,
            is_active_job_fn=self._is_active_job,
            scan_in_progress=self._scan_in_progress(),
            is_locked_job_fn=self._is_job_path_sync_locked,
        )
        if not duplicate_decision.allowed:
            return
        original_selected_ids = [self.jobs[row].id for row in rows if 0 <= row < len(self.jobs)]
        insert_at = max(rows) + 1
        duplicates: list[RenderJob] = []
        skipped_running = False
        for row in rows:
            if not (0 <= row < len(self.jobs)):
                continue
            source = self.jobs[row]
            if self._is_active_job(source):
                skipped_running = True
                continue
            clone = self._prepare_duplicate_job(source)
            if clone is None:
                continue
            duplicates.append(clone)
        if not duplicates:
            if skipped_running:
                safe_message(self, "Cannot Duplicate", "The active running job cannot be duplicated.")
            return
        for offset, clone in enumerate(duplicates):
            self.jobs.insert(insert_at + offset, clone)
        self._push_history_command(
            {
                "kind": "insert_jobs",
                "entries": [
                    {"index": insert_at + offset, "job": self._job_to_persisted_dict(job)}
                    for offset, job in enumerate(duplicates)
                ],
                "undo_select_job_ids": original_selected_ids,
                "redo_select_job_ids": [job.id for job in duplicates],
            }
        )
        self._save_and_refresh_queue(select_job_ids=[job.id for job in duplicates])
        if skipped_running:
            safe_message(self, "Some Jobs Not Duplicated", "The active running job was skipped. Other selected jobs were duplicated.")

    def _move_selected_job(self, delta: int) -> None:
        rows = self._selected_rows()
        if not rows or delta not in (-1, 1):
            return
        selected_ids = {self.jobs[r].id for r in rows if 0 <= r < len(self.jobs)}
        if not selected_ids:
            return

        moved = False
        if delta < 0:
            for i in range(1, len(self.jobs)):
                if self.jobs[i].id in selected_ids and self.jobs[i - 1].id not in selected_ids:
                    self.jobs[i - 1], self.jobs[i] = self.jobs[i], self.jobs[i - 1]
                    moved = True
        else:
            for i in range(len(self.jobs) - 2, -1, -1):
                if self.jobs[i].id in selected_ids and self.jobs[i + 1].id not in selected_ids:
                    self.jobs[i + 1], self.jobs[i] = self.jobs[i], self.jobs[i + 1]
                    moved = True

        if not moved:
            return
        self._save_and_refresh_queue(select_job_ids=list(selected_ids))

    def _on_queue_row_drag_reordered(self, source_row: int, target_row: int) -> None:
        proxy = self._queue_proxy_model()
        if proxy is not None and proxy.has_active_filters():
            self._set_status_message("Clear queue filters before reordering.", 3000)
            self._refresh_queue_table(select_job_ids=self._selected_job_ids() or None)
            return
        source_row = self._queue_source_row_from_view_row(source_row)
        target_row = self._queue_source_row_from_view_row(target_row) if target_row < self.queue_table.rowCount() else len(self.jobs)
        if not (0 <= source_row < len(self.jobs)):
            self._refresh_queue_table()
            return
        before_order = [job.id for job in self.jobs]
        selected_ids_before = self._selected_job_ids()

        # target_row is an insert position (0..len); normalize to final row index after pop.
        insert_row = max(0, min(target_row, len(self.jobs)))
        if source_row < insert_row:
            insert_row -= 1
        if insert_row < 0:
            insert_row = 0

        if source_row == insert_row:
            return

        job = self.jobs.pop(source_row)
        self.jobs.insert(insert_row, job)
        after_order = [job.id for job in self.jobs]
        self._push_history_command(
            {
                "kind": "reorder_jobs",
                "before_order": before_order,
                "after_order": after_order,
                "undo_select_job_ids": selected_ids_before,
                "redo_select_job_ids": [job.id],
            }
        )
        self._save_queue_state()
        self._refresh_queue_table(select_row=insert_row)

    def _on_queue_rows_drag_reordered(self, source_rows: list[int], target_row: int) -> None:
        proxy = self._queue_proxy_model()
        if proxy is not None and proxy.has_active_filters():
            self._set_status_message("Clear queue filters before reordering.", 3000)
            self._refresh_queue_table(select_job_ids=self._selected_job_ids() or None)
            return
        rows = self._queue_source_rows_from_view_rows([int(r) for r in source_rows])
        target_row = self._queue_source_row_from_view_row(target_row) if target_row < self.queue_table.rowCount() else len(self.jobs)
        if not rows:
            self._refresh_queue_table()
            return
        if len(rows) == 1:
            self._on_queue_row_drag_reordered(rows[0], target_row)
            return
        before_order = [job.id for job in self.jobs]
        selected_ids_before = self._selected_job_ids()

        # If dropping inside the selected block (no effective movement), keep current selection.
        min_row, max_row = rows[0], rows[-1]
        if min_row <= target_row <= max_row + 1:
            self._refresh_queue_table(select_job_ids=[self.jobs[r].id for r in rows if 0 <= r < len(self.jobs)])
            return

        selected_jobs = [self.jobs[r] for r in rows]
        selected_ids = [j.id for j in selected_jobs]

        # Remove from bottom to top to keep indexes stable.
        for r in reversed(rows):
            del self.jobs[r]

        # Adjust target insert index for removed rows above the drop position.
        removed_before_target = sum(1 for r in rows if r < target_row)
        insert_row = max(0, min(len(self.jobs), target_row - removed_before_target))

        for offset, job in enumerate(selected_jobs):
            self.jobs.insert(insert_row + offset, job)

        after_order = [job.id for job in self.jobs]
        self._push_history_command(
            {
                "kind": "reorder_jobs",
                "before_order": before_order,
                "after_order": after_order,
                "undo_select_job_ids": selected_ids_before,
                "redo_select_job_ids": selected_ids,
            }
        )
        self._save_queue_state()
        self._refresh_queue_table(select_job_ids=selected_ids)

    def _clear_finished_jobs(self) -> None:
        removed_entries = [
            {"index": idx, "job": self._job_to_persisted_dict(job)}
            for idx, job in enumerate(self.jobs)
            if job.spec.enabled is not False and job.runtime.status in {JobStatus.DONE, JobStatus.FAILED}
        ]
        self.jobs = [
            job for job in self.jobs if job.spec.enabled is False or job.runtime.status not in {JobStatus.DONE, JobStatus.FAILED}
        ]
        if removed_entries:
            self._push_history_command(
                {
                    "kind": "remove_jobs",
                    "entries": removed_entries,
                    "undo_select_job_ids": [entry["job"]["id"] for entry in removed_entries],
                    "redo_select_job_ids": [],
                }
            )
        self._save_queue_state()
        self._refresh_queue_table()

    def _refresh_queue_table(
        self,
        select_row: int | None = None,
        select_job_id: str | None = None,
        select_job_ids: list[str] | None = None,
    ) -> None:
        if self._queue_refresh_should_defer():
            self._pending_queue_refresh_args = pending_refresh_args_model(
                select_row=select_row,
                select_job_id=select_job_id,
                select_job_ids=select_job_ids,
            )
            self._pending_queue_refresh_timer.start(200)
            return
        preserved_job_id: str | None = None
        preserved_job_ids: list[str] = []
        if select_row is None and select_job_id is None and not select_job_ids:
            preserved_job_ids = [j.id for j in self._selected_jobs()]
            if not preserved_job_ids:
                selected = self._selected_job()
                if selected is not None:
                    preserved_job_id = selected.id

        selection_model = self.queue_table.selectionModel()
        selection_blocker = QtCore.QSignalBlocker(selection_model) if selection_model is not None else None
        try:
            self.queue_table.setUpdatesEnabled(False)
            self.queue_table_model.refresh_all()
            self.queue_table.clearSelection()

            target_job_id = select_job_id or preserved_job_id
            target_job_ids = list(select_job_ids or preserved_job_ids)
            target_job_id_set = set(target_job_ids)
            selected_applied = False
            if target_job_ids:
                sm = self.queue_table.selectionModel()
                if sm is not None:
                    for row, job in enumerate(self.jobs):
                        if job.id in target_job_id_set:
                            model_idx = self._queue_view_index_from_source_row(row, 0)
                            if not model_idx.isValid():
                                continue
                            sm.select(
                                model_idx,
                                QtCore.QItemSelectionModel.SelectionFlag.Select
                                | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                            )
                            selected_applied = True
            elif target_job_id:
                for row, job in enumerate(self.jobs):
                    if job.id == target_job_id:
                        model_idx = self._queue_view_index_from_source_row(row, 0)
                        if model_idx.isValid():
                            self.queue_table.selectRow(model_idx.row())
                            selected_applied = True
                        break
            elif select_row is not None and self.jobs:
                model_idx = self._queue_view_index_from_source_row(max(0, min(select_row, len(self.jobs) - 1)), 0)
                if model_idx.isValid():
                    self.queue_table.selectRow(model_idx.row())
                    selected_applied = True

            if selected_applied:
                sm = self.queue_table.selectionModel()
                row = self._selected_row()
                if sm is not None and row >= 0:
                    idx = self._queue_view_index_from_source_row(row, 0)
                    sm.setCurrentIndex(idx, QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)
        finally:
            self.queue_table.setUpdatesEnabled(True)
            if selection_blocker is not None:
                del selection_blocker

        self.queue_table.viewport().update()
        self._refresh_queue_tree_view()
        self._update_job_properties_panel()
        self._refresh_ui_state()

    def _refresh_job_row(self, job_id: str) -> None:
        target_id = str(job_id or "").strip()
        if not target_id:
            return
        row = next((idx for idx, job in enumerate(self.jobs) if job.id == target_id), -1)
        if row < 0:
            return
        if self._queue_refresh_should_defer():
            self._pending_queue_refresh_args = pending_refresh_args_model(select_job_id=target_id)
            self._pending_queue_refresh_timer.start(200)
            return
        self.queue_table_model.refresh_job_by_id(target_id)
        self.queue_table.viewport().update()
        selected_ids = set(self._selected_job_ids())
        if target_id in selected_ids:
            self._update_job_properties_panel()
        self._refresh_ui_state()

    def _refresh_queue_tree_view(self) -> None:
        if not hasattr(self, "queue_tree") or self.queue_tree is None:
            return
        tree = self.queue_tree
        model = getattr(self, "queue_tree_model", None)
        if model is None:
            return
        try:
            self._suppress_tree_item_changed = True
            refresh_queue_tree_model(tree, model, self.jobs, is_locked_job_fn=self._is_job_path_sync_locked)
        finally:
            self._suppress_tree_item_changed = False

    def _theme_icon_path(self, key: str) -> str:
        return str(getattr(self, "_theme_icons", {}).get(key, "") or "")

    def _queue_row_style_payload(self, job: RenderJob, row: int) -> dict[str, Any]:
        background = None
        foreground = None
        t = {**DEFAULT_THEME, **getattr(self, "theme", {})}
        if not job.spec.enabled:
            background = QtGui.QBrush(QtGui.QColor("#161616"))
            foreground = QtGui.QBrush(QtGui.QColor("#6f6f6f"))
        elif job.runtime.status == JobStatus.OFFLINE:
            background = QtGui.QBrush(QtGui.QColor("#2f2f2f"))
            foreground = QtGui.QBrush(QtGui.QColor("#b0b0b0"))
        elif job.runtime.status == JobStatus.RUNNING:
            background = QtGui.QBrush(QtGui.QColor(t["queue_running"]))
            foreground = QtGui.QBrush(QtGui.QColor("#ffffff"))
        elif job.runtime.status == JobStatus.DONE:
            background = QtGui.QBrush(QtGui.QColor(t["queue_done"]))
            foreground = QtGui.QBrush(QtGui.QColor("#ffffff"))
        elif job.runtime.status == JobStatus.FAILED:
            background = QtGui.QBrush(QtGui.QColor(t["queue_failed"]))
            foreground = QtGui.QBrush(QtGui.QColor("#ffffff"))
        elif job.runtime.status == JobStatus.INTERRUPTED:
            background = QtGui.QBrush(QtGui.QColor("#6b4e16"))
            foreground = QtGui.QBrush(QtGui.QColor("#ffffff"))
        return {
            "background": background,
            "foreground": foreground,
            "row": row,
        }

    def _append_log(self, source: str, text: str) -> None:
        if not text:
            return
        self.log_entries.append((source, text))
        self._append_to_log_view_if_matches(source, text)
        self._append_notifications(source, text)

    def _append_notification_message(self, message: str, severity: str = "info") -> None:
        self._push_notification_item(message, severity, dedupe_consecutive=True)

    def _push_notification_item(self, message: str, severity: str, *, dedupe_consecutive: bool) -> bool:
        normalized = normalized_notification_model(message, severity)
        signature = notification_signature_model(message, severity)
        if not should_add_notification_model(
            signature=signature,
            last_signature=self._last_notification_signature,
            dedupe_consecutive=dedupe_consecutive,
        ):
            return False
        assert normalized is not None
        text, sev = normalized
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is None:
            return False
        item = QtWidgets.QListWidgetItem(text)
        item.setIcon(self._notification_icon_for_severity(sev))
        item.setForeground(QtGui.QBrush(self._notification_color_for_severity(sev)))
        list_widget.addItem(item)
        self._last_notification_signature = signature
        self._trim_notifications_list(max_items=250)
        return True

    def _trim_notifications_list(self, *, max_items: int) -> None:
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is None:
            return
        remove_count = trim_notification_count_model(count=list_widget.count(), max_items=max_items)
        for _ in range(remove_count):
            list_widget.takeItem(0)
        if list_widget.count() > 0:
            list_widget.scrollToBottom()

    def _append_notifications(self, source: str, text: str) -> None:
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is None:
            return
        added = False
        for message, severity in self._notification_messages_for_log(source, text):
            if self._push_notification_item(message, severity, dedupe_consecutive=True):
                added = True
        if not added:
            return

    def _notification_messages_for_log(self, source: str, text: str) -> list[tuple[str, str]]:
        return notification_messages_for_log_model(source, text)

    @staticmethod
    def _notification_summary_for_line(source: str, line: str) -> tuple[str, str] | None:
        return notification_summary_for_line_model(source, line)

    @staticmethod
    def _classified_render_error_notification(low: str) -> tuple[str, str] | None:
        return classified_render_error_notification_model(low)

    def _notification_icon_for_severity(self, severity: str) -> QtGui.QIcon:
        style = self.style()
        sev = str(severity or "info").lower()
        if sev == "error":
            return style.standardIcon(QtWidgets.QStyle.StandardPixmap.SP_MessageBoxCritical)
        if sev == "warning":
            return style.standardIcon(QtWidgets.QStyle.StandardPixmap.SP_MessageBoxWarning)
        return style.standardIcon(QtWidgets.QStyle.StandardPixmap.SP_MessageBoxInformation)

    @staticmethod
    def _notification_color_for_severity(severity: str) -> QtGui.QColor:
        return QtGui.QColor(notification_color_hex_model(severity))

    def _append_to_log_view_if_matches(self, source: str, text: str) -> None:
        source_filter = self.log_source_filter.currentText()
        if source_filter != "All" and source_filter.lower() != source.lower():
            return
        needle = self.log_text_filter.text().strip().lower()
        if needle and needle not in text.lower():
            return
        cursor = self.log_output.textCursor()
        cursor.movePosition(QtGui.QTextCursor.MoveOperation.End)
        cursor.insertText(text)
        self.log_output.setTextCursor(cursor)
        self.log_output.ensureCursorVisible()

    def _refresh_log_view(self) -> None:
        self.log_output.clear()
        for source, text in self.log_entries:
            self._append_to_log_view_if_matches(source, text)

    def _clear_log_view_only(self) -> None:
        self.log_entries.clear()
        self.log_output.clear()

    def _clear_notifications_view_only(self) -> None:
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is not None:
            list_widget.clear()
        self._last_notification_signature = None

    def _build_diagnostics_snapshot(self) -> DiagnosticsSnapshot:
        status_text = ""
        if hasattr(self, "status_label") and self.status_label is not None:
            status_text = str(self.status_label.text() or "")
        return DiagnosticsSnapshot(
            app_name=APP_NAME,
            queue_path=str(self._current_queue_file_path()),
            logs_dir=str(self.config.logs_dir),
            hbatch_path=self._current_hbatch_path(),
            player_path=self._current_player_path(),
            queue_active=bool(self.queue_active),
            queue_paused=bool(self.queue_paused),
            current_job_id=str(self.current_job_id or ""),
            render_worker_active=bool(self._render_job_active() or self.render_worker_client.is_busy()),
            scan_worker_active=bool(self._scan_in_progress() or self.scan_worker_client.is_busy()),
            render_worker_stderr=self.render_worker_client.last_stderr_text,
            scan_worker_stderr=self.scan_worker_client.last_stderr_text,
            status_text=status_text,
            recovery_headline=str(self._last_recovery_headline or ""),
        )

    def _copy_diagnostics(self) -> None:
        report = build_diagnostics_report(self._build_diagnostics_snapshot())
        QtWidgets.QApplication.clipboard().setText(report)
        self._set_status_message("Diagnostics copied.", 4000)

    def _on_queue_selection_changed(self) -> None:
        self.queue_table.viewport().update()
        self._update_job_properties_panel()
        self._refresh_ui_state()

    @staticmethod
    def _parse_percent_value(text: str) -> int | None:
        return parse_percent_value_model(text)

    def _queue_progress_split_values(self, job: RenderJob) -> tuple[int | None, int | None]:
        return queue_progress_split_values_model(job)

    def _queue_header_visual_order(self) -> list[int]:
        header = self.queue_table.horizontalHeader()
        return queue_header_visual_order_model(
            column_count=self.queue_table.columnCount(),
            logical_index_for_visual=header.logicalIndex,
        )

    def _queue_hidden_columns_from_data(self, raw: Any) -> set[int]:
        return queue_hidden_columns_from_data_model(raw, column_count=self.queue_table.columnCount())

    def _queue_column_widths_from_data(self, raw: Any) -> dict[int, int]:
        return queue_column_widths_from_data_model(raw, column_count=self.queue_table.columnCount())

    def _reset_queue_view_to_defaults(self) -> None:
        default_widths = getattr(self, "_queue_default_column_widths", {})
        for logical in range(self.queue_table.columnCount()):
            width = int(default_widths.get(logical, self.queue_table.columnWidth(logical)))
            self.queue_table.setColumnWidth(logical, width)
            self.queue_table.setColumnHidden(logical, False)

    def _apply_queue_view_from_persisted_data(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            return
        widths = self._queue_column_widths_from_data(raw.get("column_widths", {}))
        for logical, width in widths.items():
            self.queue_table.setColumnWidth(logical, width)

        hidden = self._queue_hidden_columns_from_data(raw.get("hidden_columns", []))
        visible_count = self.queue_table.columnCount() - len(hidden)
        if visible_count <= 0:
            hidden.clear()
        for c in range(self.queue_table.columnCount()):
            self.queue_table.setColumnHidden(c, c in hidden)

    def _is_valid_queue_header_grouping(self) -> bool:
        header = self.queue_table.horizontalHeader()
        return is_valid_queue_header_grouping_model(
            column_count=self.queue_table.columnCount(),
            left_group={0, 1, 2, 3, 4, 5, 6},
            boundary_visual_index=6,
            is_hidden=self.queue_table.isColumnHidden,
            visual_index_for_logical=header.visualIndex,
        )

    def _restore_queue_header_order(self, visual_order: list[int]) -> None:
        header = self.queue_table.horizontalHeader()
        self._queue_header_group_restore_guard = True
        try:
            for target_visual, logical in enumerate(visual_order):
                current_visual = header.visualIndex(logical)
                if current_visual != target_visual:
                    header.moveSection(current_visual, target_visual)
        finally:
            self._queue_header_group_restore_guard = False
            self.queue_table.viewport().update()

    def _on_queue_header_section_moved(self, _logical: int, _old_visual: int, _new_visual: int) -> None:
        if self._queue_header_group_restore_guard:
            return
        if self._is_valid_queue_header_grouping():
            self._queue_header_valid_order = self._queue_header_visual_order()
            self.queue_table.viewport().update()
            return
        if self._queue_header_valid_order:
            self._restore_queue_header_order(self._queue_header_valid_order)

    def _on_queue_header_section_resized(self, _logical: int, _old_size: int, _new_size: int) -> None:
        self._save_queue_state()

    def _show_queue_header_context_menu(self, pos: QtCore.QPoint) -> None:
        header = self.queue_table.horizontalHeader()
        if header is None:
            return

        logical_clicked = header.logicalIndexAt(pos)
        menu = QtWidgets.QMenu(self)

        act_hide = menu.addAction("Hide")
        can_hide_clicked = bool(
            logical_clicked >= 0
            and logical_clicked < self.queue_table.columnCount()
            and not self.queue_table.isColumnHidden(logical_clicked)
        )
        visible_count = sum(0 if self.queue_table.isColumnHidden(c) else 1 for c in range(self.queue_table.columnCount()))
        if visible_count <= 1:
            can_hide_clicked = False
        act_hide.setEnabled(can_hide_clicked)

        menu.addSeparator()

        toggle_actions: dict[QtGui.QAction, int] = {}
        for logical in range(self.queue_table.columnCount()):
            label = str(
                self.queue_table.model().headerData(
                    logical,
                    QtCore.Qt.Orientation.Horizontal,
                    QtCore.Qt.ItemDataRole.DisplayRole,
                )
                or f"Column {logical + 1}"
            )
            act = menu.addAction(label)
            act.setCheckable(True)
            is_visible = not self.queue_table.isColumnHidden(logical)
            act.setChecked(is_visible)
            if is_visible and visible_count <= 1:
                act.setEnabled(False)
            toggle_actions[act] = logical

        chosen = menu.exec(header.mapToGlobal(pos))
        if chosen is None:
            return

        if chosen == act_hide:
            if can_hide_clicked:
                self.queue_table.setColumnHidden(int(logical_clicked), True)
                self._save_queue_state()
                self.queue_table.viewport().update()
            return

        logical = toggle_actions.get(chosen)
        if logical is None:
            return
        should_show = bool(chosen.isChecked())
        currently_visible = not self.queue_table.isColumnHidden(logical)
        if should_show == currently_visible:
            return
        if not should_show and visible_count <= 1:
            return
        self.queue_table.setColumnHidden(logical, not should_show)
        self._save_queue_state()
        self.queue_table.viewport().update()

    def _queue_model_display_text(self, row: int, column: int) -> str:
        model = getattr(self, "queue_table_model", None)
        if model is None:
            return ""
        index = model.index(row, column)
        if not index.isValid():
            return ""
        return str(index.data(QtCore.Qt.ItemDataRole.DisplayRole) or "").strip()

    def _apply_queue_cell_edit(self, row: int, col: int, text: str, selected_rows_override: list[int] | None = None) -> bool:
        return apply_queue_cell_edit_model(
            self,
            row,
            col,
            text,
            selected_rows_override=selected_rows_override,
            show_message=lambda title, message, details=None: safe_message(self, title, message, details),
        )

    def _on_queue_frame_handling_chosen(self, row: int, text: str) -> None:
        if not (0 <= row < len(self.jobs)):
            return
        consume_rows = getattr(self.queue_table, "consume_frame_handling_target_rows", None)
        selected_rows = list(consume_rows()) if callable(consume_rows) else None
        self._apply_queue_cell_edit(row, 5, text, selected_rows_override=selected_rows)

    def _handle_scan_requested(self, request: dict) -> None:
        started = self.scan_coordinator.handle_scan_requested(request)
        self._create_job_scan_in_progress = bool(started)
        self._refresh_ui_state()

    def _handle_scan_worker_message(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("request_id", "") or "")
        if request_id and self._active_scan_request_id and request_id != self._active_scan_request_id:
            return
        payload = dict(message.get("payload", {}) or {})
        message_type = str(message.get("type", "") or "")
        if message_type == "scan.result":
            self._active_scan_request_id = ""
            self._create_job_scan_in_progress = False
            records = list(payload.get("records", []) or [])
            renderable_records = [r for r in records if self._is_likely_renderable_scan_node(r)]
            selected_records = renderable_records or records
            if selected_records:
                self.add_job_panel.set_scanned_rops(selected_records)
                if renderable_records:
                    self._set_status_message(
                        f"Scan complete ({len(renderable_records)} likely render nodes, {len(records)} total)",
                        5000,
                    )
                else:
                    self._append_log("Stdout", "[Scan] No likely render/output nodes matched; showing all scanned nodes.\n")
                    self._set_status_message(f"Scan complete ({len(records)} nodes found, unfiltered)", 5000)
            else:
                safe_message(self, "Scan", "No nodes found in selected scan targets.")
                self._set_status_message("No nodes found in selected scan targets.", 5000)
            self._refresh_ui_state()
            return
        if message_type == "scan.failed":
            self._active_scan_request_id = ""
            self._create_job_scan_in_progress = False
            message_text = str(payload.get("message", "") or "Scan failed.")
            details = str(payload.get("stderr", "") or self.scan_worker_client.last_stderr_text or "")
            safe_message(self, "Scan", message_text, details or None)
            self._set_status_message(message_text, 5000)
            self._refresh_ui_state()

    @staticmethod
    def _is_likely_renderable_scan_node(record: dict[str, str]) -> bool:
        path = str(record.get("path", ""))
        category = str(record.get("category", ""))
        type_name = str(record.get("type_name", ""))
        if category.lower() == "driver":
            return True
        if path.startswith("/stage/"):
            if type_name in {"usdrender_rop"}:
                return True
            if type_name.endswith("_rop") and ("render" in type_name.lower() or "usd" in type_name.lower()):
                return True
        return False

    def _queue_lifecycle_state(self) -> QueueLifecycleState:
        return QueueLifecycleState(
            queue_active=bool(self.queue_active),
            queue_paused=bool(self.queue_paused),
            stop_requested=bool(self.stop_requested),
            canceling_current_job=bool(self.canceling_current_job),
            current_job_id=self.current_job_id,
            active_hbatch_pid=int(self._active_hbatch_pid or 0),
            queue_rerun_statuses=set(self._queue_rerun_statuses),
            jobs_started_this_run=set(self._jobs_started_this_run),
            queue_next_search_index=int(self._queue_next_search_index),
        )

    def _apply_queue_lifecycle_state(self, state: QueueLifecycleState) -> None:
        self.queue_active = bool(state.queue_active)
        self.queue_paused = bool(state.queue_paused)
        self.stop_requested = bool(state.stop_requested)
        self.canceling_current_job = bool(state.canceling_current_job)
        self.current_job_id = state.current_job_id
        self._active_hbatch_pid = int(state.active_hbatch_pid)
        self._queue_rerun_statuses = set(state.queue_rerun_statuses)
        self._jobs_started_this_run = set(state.jobs_started_this_run)
        self._queue_next_search_index = int(state.queue_next_search_index)

    def _start_queue(self) -> None:
        selected = self._selected_job()
        can_start_selected = self._is_job_runnable(selected)
        has_runnable = any(self._is_job_runnable(job) for job in self.jobs)
        start_decision = evaluate_start_request_model(
            self._queue_lifecycle_state(),
            hbatch_exists=self._hbatch_exists(),
            has_runnable=has_runnable,
            can_start_selected=can_start_selected,
        )
        if self.queue_active:
            if self.queue_paused and start_decision.resume_existing:
                self._apply_queue_lifecycle_state(with_queue_resumed_model(self._queue_lifecycle_state()))
                self._append_log("Stdout", "\n[Queue] Resumed\n")
                self._set_status_message("Queue resumed", 3000)
                self._maybe_start_next_job()
                self._refresh_ui_state()
            return
        if not bool(start_decision.allowed):
            reason = str(start_decision.reason or "")
            title = "hbatch Missing" if "hbatch" in reason.lower() else "Queue Empty"
            safe_message(self, title, reason)
            return
        self._write_queue_snapshot("before_start")
        self._apply_queue_lifecycle_state(with_queue_started_model(self._queue_lifecycle_state()))
        self._append_log("Stdout", "\n=== Queue Started ===\n")
        self._set_status_message("Queue started")
        self._refresh_ui_state()
        if can_start_selected:
            if selected is not None and selected.runtime.status not in {JobStatus.RUNNING, JobStatus.QUEUED}:
                self._queue_rerun_statuses = {selected.runtime.status}
            self._start_job(selected)
        else:
            self._maybe_start_next_job()

    def _toggle_pause(self) -> None:
        if not self.queue_active:
            return
        self._apply_queue_lifecycle_state(with_pause_toggled_model(self._queue_lifecycle_state()))
        if self.queue_paused:
            self._append_log("Stdout", "\n[Queue] Pause requested (takes effect between jobs)\n")
            self._set_status_message("Queue will pause after current job")
        else:
            self._append_log("Stdout", "\n[Queue] Resumed\n")
            self._set_status_message("Queue resumed", 3000)
            self._maybe_start_next_job()
        self._refresh_ui_state()

    def _stop_queue(self) -> None:
        if not self.queue_active and not self._render_job_active():
            return
        self._apply_queue_lifecycle_state(
            with_stop_requested_model(self._queue_lifecycle_state(), render_job_active=self._render_job_active())
        )
        self._append_log("Stdout", "\n[Queue] Stop requested\n")
        self._set_status_message("Stopping queue...")
        if self._render_job_active():
            self._append_log("Stdout", "[Queue] Terminating current render process...\n")
            self._send_render_worker_request("render.stop", {}, request_id=self._active_render_request_id or uuid4().hex)
            self._ensure_kill_timer()
        else:
            self._finish_queue("Queue stopped")
        self._refresh_ui_state()

    def _ensure_kill_timer(self) -> None:
        if self._pending_kill_timer is None:
            self._pending_kill_timer = QtCore.QTimer(self)
            self._pending_kill_timer.setSingleShot(True)
            self._pending_kill_timer.timeout.connect(self._force_kill_render_process)
        self._pending_kill_timer.start(3000)

    def _force_kill_render_process(self) -> None:
        if self._render_job_active():
            self._append_log("Stderr", "[Queue] Force killing current render process.\n")
            self._send_render_worker_request("render.kill", {}, request_id=self._active_render_request_id or uuid4().hex)

    def _maybe_start_next_job(self) -> None:
        decision = decide_next_job_model(
            self._queue_lifecycle_state(),
            jobs=self.jobs,
            render_job_active=self._render_job_active(),
            is_runnable=self._is_job_runnable,
        )
        if decision.finish_message:
            self._finish_queue(decision.finish_message)
            return
        if decision.job is None:
            return
        self._start_job(decision.job)

    def _start_job(self, job: RenderJob) -> None:
        if not self._hbatch_exists():
            safe_message(self, "hbatch Missing", "Configured hbatch.exe no longer exists.")
            self._finish_queue("Queue aborted")
            return
        if not Path(job.spec.hip_path).exists():
            self._mark_job_offline(job, "HIP file not found.")
            self._save_queue_state()
            self._refresh_queue_table(select_job_id=job.id)
            self._maybe_start_next_job()
            return
        start_job_runtime_model(self, job)

    def _ensure_husk_hook_files(self) -> dict[str, str]:
        try:
            return ensure_husk_hook_files_model(
                scripts_dir=self._project_houdini_scripts_dir(),
                hook_script_path_fn=self.config.hook_script_path,
            )
        except Exception as exc:
            self._append_log("Stderr", f"[Preflight] Failed to create husk hook files: {exc}\n")
            safe_message(self, "Missing Houdini Scripts", str(exc))
            return {}

    def _current_job(self) -> RenderJob | None:
        if not self.current_job_id:
            return None
        for job in self.jobs:
            if job.id == self.current_job_id:
                return job
        return None

    def _handle_render_worker_message(self, message: dict[str, Any]) -> None:
        handle_render_worker_message_model(self, message)

    def _handle_render_worker_crash(self, reason: str) -> None:
        handle_render_worker_crash_model(self, reason)

    def _update_job_progress_from_output(self, text: str) -> None:
        update_job_progress_from_output_model(self, text)

    @staticmethod
    def _total_frames_for_job(job: RenderJob) -> int | None:
        return total_frames_for_job_model(job)

    @staticmethod
    def _update_job_render_timing_stats(job: RenderJob) -> None:
        update_job_render_timing_stats_model(job, format_duration_short_fn=format_duration_short_model)

    def _update_phase_from_frame_sequence(self, job: RenderJob, previous_frame_seen: float | None) -> None:
        if job.runtime.allframesatonce_enabled is not True:
            return
        if job.view.last_frame_seen is None:
            return
        if job.view.phase_text == "Render":
            self._cancel_phase_promote()
            return

        current = job.view.last_frame_seen
        range_start: float | None = None
        range_end: float | None = None
        range_step: float = 1.0

        if job.spec.frame_range_mode == "override" and job.spec.start_frame is not None and job.spec.end_frame is not None:
            range_start = float(job.spec.start_frame)
            range_end = float(job.spec.end_frame)
            range_step = float(job.spec.step or 1)
        elif job.runtime.runtime_start_frame is not None and job.runtime.runtime_end_frame is not None:
            range_start = float(job.runtime.runtime_start_frame)
            range_end = float(job.runtime.runtime_end_frame)
            range_step = float(job.runtime.runtime_step or 1.0)

        if range_start is not None and range_end is not None:
            # Mark first pass (USD build) complete once the frame sequence reaches the end.
            if current >= range_end:
                if not job.view.build_pass_completed:
                    job.view.build_pass_completed = True
                    self._schedule_phase_promote(job.id)
            # If frames restart after the build pass, render phase has definitely started.
            if (
                job.view.build_pass_completed
                and previous_frame_seen is not None
                and current < previous_frame_seen
                and current <= float(range_start + (range_step * 2))
            ):
                job.view.phase_text = "Render"
                self._cancel_phase_promote()
                return
        else:
            # Fallback heuristic without a known range: restart implies next pass.
            if job.view.phase_text == "USD Build" and previous_frame_seen is not None and current < previous_frame_seen:
                job.view.phase_text = "Render"
                self._cancel_phase_promote()

    def _update_job_phase_from_output(self, job: RenderJob, text: str) -> None:
        phase = detect_phase_from_output_with_job_model(job, text)
        if not phase:
            return
        if phase == "USD Build" and job.runtime.allframesatonce_enabled is False:
            return
        # Once actual rendering starts, keep that phase unless the job restarts.
        if job.view.phase_text == "Render" and phase != "Render":
            return
        job.view.phase_text = phase

    def _schedule_phase_promote(self, job_id: str) -> None:
        if self._phase_promote_timer is None:
            self._phase_promote_timer = QtCore.QTimer(self)
            self._phase_promote_timer.setSingleShot(True)
            self._phase_promote_timer.timeout.connect(self._promote_phase_to_render_if_still_running)
        self._phase_promote_job_id = job_id
        self._phase_promote_timer.start(1200)

    def _cancel_phase_promote(self) -> None:
        self._phase_promote_job_id = None
        if self._phase_promote_timer is not None:
            self._phase_promote_timer.stop()

    def _promote_phase_to_render_if_still_running(self) -> None:
        job = self._current_job()
        if job is None:
            return
        if self._phase_promote_job_id != job.id:
            return
        if not self._render_job_active():
            return
        if job.runtime.allframesatonce_enabled is True and job.view.build_pass_completed and job.view.phase_text != "Render":
            job.view.phase_text = "Render"
            self._refresh_job_row(job.id)

    def _on_render_finished(self, exit_code: int, exit_status: QtCore.QProcess.ExitStatus) -> None:
        on_render_finished_model(self, exit_code, exit_status)

    def _finish_queue(self, message: str) -> None:
        new_state, started_job_ids = with_queue_finished_model(self._queue_lifecycle_state())
        self._apply_queue_lifecycle_state(new_state)
        self._set_status_message(message, 5000)
        self._append_log("Stdout", f"\n=== {message} ===\n")
        summary = self._build_queue_run_summary(started_job_ids)
        if summary is not None:
            msg, severity = summary
            self._append_notification_message(msg, severity)
        self._refresh_queue_table()

    def _kill_process_tree_by_pid(self, pid: int) -> bool:
        if pid <= 0 or not sys.platform.startswith("win"):
            return False
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.SubprocessError):
            return False
        output = f"{result.stdout}\n{result.stderr}".lower()
        return result.returncode == 0 or "not found" in output or "no running instance" in output

    def _kill_active_hbatch_tree(self) -> bool:
        pid = int(self._active_hbatch_pid or 0)
        if pid <= 0:
            return False
        killed = self._kill_process_tree_by_pid(pid)
        if killed:
            self._active_hbatch_pid = 0
        return killed

    def _write_job_log(self, text: str) -> None:
        try:
            if self.current_job_log_handle is not None:
                self.current_job_log_handle.write(text)
                self.current_job_log_handle.flush()
        except Exception as exc:
            _log_suppressed_exception("MainWindow._write_job_log", exc)

    def _close_current_job_log(self) -> None:
        try:
            if self.current_job_log_handle is not None:
                self.current_job_log_handle.close()
        except Exception as exc:
            _log_suppressed_exception("MainWindow._close_current_job_log", exc)
        self.current_job_log_handle = None

    def _open_selected_job_log(self) -> None:
        job = self._selected_job()
        if not job or not job.runtime.log_file_path:
            return
        path = Path(job.runtime.log_file_path)
        if not path.exists():
            safe_message(self, "Log Missing", f"Log file does not exist:\n{path}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(path)))

    def _job_preview_path(self, job: RenderJob) -> Path | None:
        candidate = str(job.view.out_file_sample_path or "").strip()
        if not candidate or candidate.lower() == "ip":
            return None

        direct_path = Path(candidate)
        if direct_path.exists() and direct_path.is_file():
            return direct_path

        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        if resolved is not None:
            start_frame, end_frame, step = resolved
            for frame in range(start_frame, end_frame + 1, step):
                seq_path = self._frame_sequence_path_for_frame(candidate, frame)
                if seq_path is not None and seq_path.exists() and seq_path.is_file():
                    return seq_path
            fallback_seq = self._frame_sequence_path_for_frame(candidate, start_frame)
            if fallback_seq is not None:
                return fallback_seq

        return direct_path if direct_path.suffix else None

    def _preview_job(self, job: RenderJob) -> None:
        preview_path = self._job_preview_path(job)
        player_path = self._current_player_path()
        player = Path(player_path) if player_path else Path()
        preview_decision = validate_preview_launch(
            preview_path_exists=preview_path is not None,
            player_path_set=bool(player_path),
            player_exists=bool(player_path and player.exists()),
        )
        if not preview_decision.valid:
            if preview_decision.message == "Preview player does not exist." and player_path:
                safe_message(self, "Preview", f"Preview player does not exist:\n{player}")
            else:
                safe_message(self, preview_decision.title or "Preview", preview_decision.message)
            return
        assert preview_path is not None
        try:
            started = QtCore.QProcess.startDetached(str(player), [str(preview_path)])
        except (RuntimeError, TypeError) as exc:
            safe_message(self, "Preview", f"Failed to launch preview player:\n{player}", str(exc))
            return
        if not started:
            safe_message(self, "Preview", f"Failed to launch preview player:\n{player}")

    def _open_logs_folder(self) -> None:
        folder = self.config.logs_dir
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            decision = validate_logs_folder_access(folder_ready=False, create_failed=True)
            safe_message(self, decision.title or "Logs Folder", f"{decision.message}\n{folder}", str(exc))
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))

    def _clear_log_files(self) -> None:
        busy_decision = validate_log_file_deletion(
            logs_busy=bool(self.current_job_log_handle is not None or self._render_job_active()),
            has_logs=True,
        )
        if not busy_decision.valid and busy_decision.title == "Logs Busy":
            safe_message(self, busy_decision.title, busy_decision.message)
            return
        logs_dir = self.config.logs_dir
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            decision = validate_logs_folder_access(folder_ready=False, create_failed=False)
            safe_message(self, decision.title or "Logs Folder", f"{decision.message}\n{logs_dir}", str(exc))
            return

        log_paths = sorted(p for p in logs_dir.glob("*.log") if p.is_file())
        has_logs_decision = validate_log_file_deletion(logs_busy=False, has_logs=bool(log_paths))
        if not has_logs_decision.valid:
            safe_message(self, has_logs_decision.title or "Logs", has_logs_decision.message)
            return

        answer = QtWidgets.QMessageBox.question(
            self,
            "Delete Log Files",
            f"Delete {len(log_paths)} log file(s) from:\n{logs_dir}?",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
        )
        if answer != QtWidgets.QMessageBox.StandardButton.Yes:
            return

        deleted = 0
        failed: list[str] = []
        for path in log_paths:
            try:
                path.unlink()
                deleted += 1
            except OSError as exc:
                failed.append(f"{path.name}: {exc}")

        if failed:
            safe_message(
                self,
                "Logs",
                f"Deleted {deleted} log file(s), but {len(failed)} failed.",
                "\n".join(failed[:20]),
            )
        else:
            safe_message(self, "Logs", f"Deleted {deleted} log file(s).")

    def _open_selected_job_output_folder(self) -> None:
        job = self._selected_job()
        if not job:
            return
        folder = self._output_folder_from_value(job.view.out_path) or Path(job.spec.hip_path).parent
        decision = validate_output_folder_open(folder_exists=folder.exists())
        if not decision.valid:
            safe_message(self, decision.title or "Folder Missing", f"{decision.message}\n{folder}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))

    def _refresh_ui_state(self) -> None:
        running = self.queue_active and self._render_job_active()
        scan_in_progress = self._scan_in_progress()
        create_job_scan_in_progress = bool(self._create_job_scan_in_progress)
        hbatch_ok = self._hbatch_exists()
        path_sync_in_progress = bool(self._path_sync_lock_counts)

        self.add_job_panel.set_enabled_for_run_state(self.queue_active, create_job_scan_in_progress)
        self.btn_preferences.setEnabled(not scan_in_progress)
        if hasattr(self, "btn_reload_all_tree") and self.btn_reload_all_tree is not None:
            self.btn_reload_all_tree.setEnabled(hbatch_ok and not scan_in_progress and not self._render_job_active() and not path_sync_in_progress)
        experimental_chunking_enabled = self._experimental_chunking_enabled()
        self.chk_enable_chunking.setVisible(experimental_chunking_enabled)
        self.spin_chunk_size.setVisible(experimental_chunking_enabled)
        if not experimental_chunking_enabled and self.chk_enable_chunking.isChecked():
            self.chk_enable_chunking.setChecked(False)

        has_queued = any(self._is_job_runnable(j) for j in self.jobs)
        selected = self._selected_job()
        can_start_selected = self._is_job_runnable(selected)
        self.btn_start_queue.setEnabled(
            hbatch_ok and (has_queued or can_start_selected or (self.queue_active and self.queue_paused))
        )
        self.btn_pause_queue.setEnabled(self.queue_active)
        self.btn_pause_queue.setText("Resume" if self.queue_paused else "Pause")
        self.btn_stop_queue.setEnabled(self.queue_active or self._render_job_active())
        self.queue_file_menu_button.setEnabled(not scan_in_progress and not self._render_job_active() and not path_sync_in_progress)
        self.chk_disable_husk_mplay.setEnabled(not self.queue_active and not self._render_job_active())
        self.chk_enable_chunking.setEnabled(experimental_chunking_enabled and not self.queue_active and not self._render_job_active())
        self.spin_chunk_size.setEnabled(
            experimental_chunking_enabled and not self.queue_active and not self._render_job_active() and self.chk_enable_chunking.isChecked()
        )
        self.spin_auto_retry.setEnabled(not self.queue_active and not self._render_job_active())
        self.spin_retry_delay.setEnabled(not self.queue_active and not self._render_job_active())

        selected = self._selected_job()
        self.btn_open_log_file.setEnabled(bool(selected and selected.log_file_path))
        self._update_job_properties_panel()

        if running:
            self._set_status_message("Rendering...")
        elif self.queue_active and self.queue_paused:
            self._set_status_message("Queue paused")
        elif create_job_scan_in_progress:
            self._set_status_message("Scanning /out ...")
        elif path_sync_in_progress:
            self._set_status_message("Updating path...")

    def _reload_all_jobs_from_files(self) -> None:
        if self._scan_in_progress():
            return
        target_jobs = [job for job in self.jobs if job.runtime.status != JobStatus.RUNNING]
        if not target_jobs:
            self._set_status_message("No jobs to reload from file.", 3000)
            return
        self._write_queue_snapshot("before_reload_all")
        self._defer_reload_jobs_from_file(
            target_jobs,
            reset_override_to_rop=False,
            status_text="Reloading all jobs from file...",
            notification_label="Reload All",
        )

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        if self._render_job_active():
            answer = QtWidgets.QMessageBox.question(
                self,
                "Render Running",
                "A render is currently running. Stop the process and exit?",
                QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            )
            if answer != QtWidgets.QMessageBox.StandardButton.Yes:
                event.ignore()
                return
            self.stop_requested = True
            self.canceling_current_job = True
            self._kill_active_hbatch_tree()
            self._send_render_worker_request("render.kill", {}, request_id=self._active_render_request_id or uuid4().hex)

        self.scan_worker_client.shutdown()
        self.background_scan_worker_client.shutdown()
        self.render_worker_client.shutdown()

        self._close_current_job_log()
        self._save_queue_state()
        if self._main_splitter_left_width_pref is not None:
            self.config.set("main_splitter_left_width", int(self._main_splitter_left_width_pref))
        if self._left_splitter_top_height_pref is not None:
            self.config.set("left_splitter_top_height", int(self._left_splitter_top_height_pref))
        if self._left_notifications_height_pref is not None:
            self.config.set("left_notifications_height", int(self._left_notifications_height_pref))
        self.config.set("hbatch_path", self._current_hbatch_path())
        super().closeEvent(event)

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._apply_main_splitter_left_width_pref()
        self._apply_left_splitter_default_sizes()
        self._apply_left_column_splitter_default_sizes()


def install_excepthook() -> None:
    def _hook(exc_type, exc, tb) -> None:
        msg = "".join(traceback.format_exception(exc_type, exc, tb))
        try:
            QtWidgets.QMessageBox.critical(None, "Unhandled Error", msg)
        except Exception as msgbox_exc:
            _log_suppressed_exception("install_excepthook._hook.messagebox", msgbox_exc)
        sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _hook


def create_app() -> QtWidgets.QApplication:
    QtCore.QCoreApplication.setOrganizationName(ORG_NAME)
    QtCore.QCoreApplication.setApplicationName(APP_NAME)
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setWindowIcon(MainWindow._build_app_icon())
    return app


def main() -> int:
    install_excepthook()
    app = create_app()
    win = MainWindow()
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

