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

from PySide6 import QtCore, QtGui, QtSvg, QtWidgets
from app_core.action_policy import (
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
from flows.app_preferences_flow import (
    dialog_device_defaults as dialog_device_defaults_model,
    dialog_experimental_flags as dialog_experimental_flags_model,
    dialog_runtime_defaults as dialog_runtime_defaults_model,
    parse_preferences_payload as parse_preferences_payload_model,
)
from app_core.atomic_io import read_json_file, write_json_atomic
from app_core.diagnostics import DiagnosticsSnapshot, build_diagnostics_report
from app_core.diagnostics_snapshot_builder import build_diagnostics_snapshot as build_diagnostics_snapshot_model
from houdini_core.houdini_service import (
    build_render_preflight_script as build_render_preflight_script_model,
    ensure_husk_hook_files as ensure_husk_hook_files_model,
    load_houdini_script_text as load_houdini_script_text_model,
    project_houdini_scripts_dir as project_houdini_scripts_dir_model,
    required_houdini_script_filenames as required_houdini_script_filenames_model,
    validate_houdini_script_files as validate_houdini_script_files_model,
)
from app_core.job_validation import (
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
from queue_core.queue_editing import (
    clear_job_resume_runtime_state as clear_job_resume_runtime_state_model,
    mark_job_offline as mark_job_offline_model,
    reset_job_state as reset_job_state_model,
    restore_job_online_status as restore_job_online_status_model,
)
from queue_core.queue_file_controller import QueueFileController, QueueFileControllerHooks
from queue_core.queue_models import DeviceOverrideMode, FrameHandlingMode, JobStatus, RenderJob, UsdOutputDirectoryMode
from queue_core.queue_persistence import (
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
from queue_core.queue_execution import (
    advance_job_to_next_chunk as advance_job_to_next_chunk_model,
    retry_current_chunk as retry_current_chunk_model,
)
from app_core.log_panel_actions import (
    delete_log_files as delete_log_files_model,
    discover_log_files as discover_log_files_model,
    log_deletion_feedback as log_deletion_feedback_model,
    selected_job_log_path as selected_job_log_path_model,
)
from queue_core.queue_filter_proxy import QueueFilterProxyModel, QUEUE_STATUS_FILTER_OPTIONS
from queue_core.queue_cell_editing import apply_queue_cell_edit as apply_queue_cell_edit_model
from queue_core.queue_run_executor import (
    handle_render_worker_crash as handle_render_worker_crash_model,
    handle_render_worker_message as handle_render_worker_message_model,
    on_render_finished as on_render_finished_model,
    start_job_runtime as start_job_runtime_model,
    update_job_progress_from_output as update_job_progress_from_output_model,
)
from queue_core.queue_runtime_state import (
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
from queue_core.queue_lifecycle import (
    QueueLifecycleState,
    decide_next_job as decide_next_job_model,
    evaluate_start_request as evaluate_start_request_model,
    with_pause_toggled as with_pause_toggled_model,
    with_queue_finished as with_queue_finished_model,
    with_queue_resumed as with_queue_resumed_model,
    with_queue_started as with_queue_started_model,
    with_stop_requested as with_stop_requested_model,
)
from queue_core.queue_run_reporting import build_queue_run_summary as build_queue_run_summary_model
from queue_core.queue_run_reporting import write_queue_snapshot as write_queue_snapshot_model
from flows.queue_reload_flow import (
    defer_reload_values_from_file as defer_reload_values_from_file_model,
    run_reload_all_jobs_from_file as run_reload_all_jobs_from_file_model,
)
from queue_core.queue_path_sync_tasks import (
    enqueue_path_sync_task as enqueue_path_sync_task_model,
    run_next_path_sync_task as run_next_path_sync_task_model,
    should_schedule_next_path_sync_task as should_schedule_next_path_sync_task_model,
)
from queue_core.queue_path_sync_lock import (
    advance_path_sync_overlay as advance_path_sync_overlay_model,
    begin_path_sync_lock as begin_path_sync_lock_model,
    end_path_sync_lock as end_path_sync_lock_model,
    is_job_path_sync_locked as is_job_path_sync_locked_model,
)
from queue_core.queue_view_state_coordinator import QueueViewStateCoordinator
from queue_core.queue_output_paths import (
    frame_sequence_path_for_frame as frame_sequence_path_for_frame_model,
    normalize_output_display_path as normalize_output_display_path_model,
    output_folder_from_value as output_folder_from_value_model,
)
from queue_core.queue_context_menu_coordinator import QueueContextMenuCoordinator
from queue_core.queue_refresh_coordinator import QueueRefreshCoordinator
from queue_core.queue_tree_context_menu_coordinator import QueueTreeContextMenuCoordinator
from app_core.preview_paths import resolve_job_preview_path as resolve_job_preview_path_model
from queue_core.queue_frame_scan import (
    first_missing_frame_and_contiguous_done as first_missing_frame_and_contiguous_done_model,
    missing_frame_runs_and_existing_count as missing_frame_runs_and_existing_count_model,
)
from queue_core.queue_history import (
    apply_history_command as apply_history_command_model,
    bounded_undo_stack as bounded_undo_stack_model,
    history_command_targets_job as history_command_targets_job_model,
    should_push_history_command as should_push_history_command_model,
)
from queue_core.queue_progress_state import (
    job_phase_display as job_phase_display_model,
    parse_percent_value as parse_percent_value_model,
    queue_progress_split_values as queue_progress_split_values_model,
)
from usd_core.usd_queue_status import (
    usd_status_display as usd_status_display_model,
    usd_status_tooltip as usd_status_tooltip_model,
)
from queue_core.queue_table_model import QueueTableModel, QueueTableModelHooks
from queue_core.queue_tree_ui import (
    TREE_HIP_ROLE,
    TREE_KIND_ROLE,
    TREE_ROP_ROLE,
    TREE_USED_ROLE,
    build_queue_tree_panel as build_queue_tree_panel_model,
)
from queue_core.queue_tree_sync import (
    propagate_hip_path_change as propagate_hip_path_change_model,
    propagate_rop_path_change as propagate_rop_path_change_model,
    refresh_jobs_from_rop_metadata as refresh_jobs_from_rop_metadata_model,
    sync_jobs_after_path_change as sync_jobs_after_path_change_model,
    validate_queue_path_value as validate_queue_path_value_model,
)
from queue_core.queue_path_change_orchestration import (
    affected_job_ids_for_hip_path_change as affected_job_ids_for_hip_path_change_model,
    affected_job_ids_for_rop_path_change as affected_job_ids_for_rop_path_change_model,
    apply_hip_path_change_immediately as apply_hip_path_change_immediately_model,
    apply_rop_path_change_immediately as apply_rop_path_change_immediately_model,
    defer_finalize_path_change as defer_finalize_path_change_model,
    defer_reload_jobs_from_file as defer_reload_jobs_from_file_model,
)
from job_core.job_properties_actions import (
    JobPropertyEditSpec,
    device_mode_edit_spec as device_mode_edit_spec_model,
    device_selection_edit_spec as device_selection_edit_spec_model,
    retain_built_usd_edit_spec as retain_built_usd_edit_spec_model,
    reuse_retained_usd_edit_spec as reuse_retained_usd_edit_spec_model,
    single_process_render_edit_spec as single_process_render_edit_spec_model,
    usd_output_directory_custom_path_edit_spec as usd_output_directory_custom_path_edit_spec_model,
    usd_output_directory_mode_edit_spec as usd_output_directory_mode_edit_spec_model,
)
from job_core.job_properties_state import (
    default_job_properties_panel_state as default_job_properties_panel_state_model,
)
from flows.job_properties_panel_flow import (
    build_job_properties_state_for_selection as build_job_properties_state_for_selection_model,
)
from render_core.render_session import RenderSessionController, RenderSessionHooks
from houdini_core.scan_coordinator import ScanCoordinator, ScanCoordinatorHooks
from houdini_core.tree_scan_coordinator import TreeScanCoordinator
from render_core.render_output_parser import (
    detect_phase_from_output_with_job as detect_phase_from_output_with_job_model,
)
from houdini_core.rop_metadata import (
    RopInfo,
    apply_rop_info_to_job as apply_rop_info_to_job_model,
)
from usd_core.retained_usd_policy import (
    retained_usd_build_info as retained_usd_build_info_model,
    retained_usd_built_at_text as retained_usd_built_at_text_model,
    retained_usd_hip_stale_reason as retained_usd_hip_stale_reason_model,
    retained_usd_invalid_reason as retained_usd_invalid_reason_model,
    retained_usd_metadata_path as retained_usd_metadata_path_model,
    retained_usd_status_text as retained_usd_status_text_model,
)
from usd_core.retained_usd_runtime import (
    clear_retained_usd_runtime as clear_retained_usd_runtime_model,
    delete_retained_usd_folder_for_job as delete_retained_usd_folder_for_job_model,
    is_absolute_retained_usd_path as is_absolute_retained_usd_path_model,
    selected_retained_usd_paths as selected_retained_usd_paths_model,
    should_write_retained_usd_metadata_now as should_write_retained_usd_metadata_now_model,
    sync_retained_usd_file_state as sync_retained_usd_file_state_model,
    write_retained_usd_metadata as write_retained_usd_metadata_model,
)
from usd_core.retained_usd_panel_state import (
    retained_usd_panel_default_fields as retained_usd_panel_default_fields_model,
    single_job_retained_usd_panel_state as single_job_retained_usd_panel_state_model,
)
from usd_core.retained_usd_actions import (
    clear_deleted_retained_usd_runtime as clear_deleted_retained_usd_runtime_model,
    delete_retained_usd_directories as delete_retained_usd_directories_model,
    first_retained_usd_folder as first_retained_usd_folder_model,
)
from app_core.notification_rules import (
    classified_render_error_notification as classified_render_error_notification_model,
    notification_messages_for_log as notification_messages_for_log_model,
    notification_summary_for_line as notification_summary_for_line_model,
)
from app_core.notification_coordinator import (
    appendable_notifications as appendable_notifications_model,
    appendable_notifications_for_log as appendable_notifications_for_log_model,
)
from app_core.notification_list_state import (
    notification_color_hex as notification_color_hex_model,
    trim_notification_count as trim_notification_count_model,
)
from queue_core.queue_selection_helpers import (
    mixed_value as mixed_value_model,
    selected_row_from_view_rows as selected_row_from_view_rows_model,
    source_rows_from_view_rows as source_rows_from_view_rows_model,
)
from queue_core.queue_job_paths import (
    configured_retained_usd_folder_preview as configured_retained_usd_folder_preview_model,
    job_file_name_from_path as job_file_name_from_path_model,
    job_rop_name_from_path as job_rop_name_from_path_model,
    safe_usd_folder_name as safe_usd_folder_name_model,
)
from queue_core.queue_targeting import (
    current_job_by_id as current_job_by_id_model,
    selected_job_for_row as selected_job_for_row_model,
    tree_context_target_jobs as tree_context_target_jobs_model,
)
from queue_core.queue_model_text import queue_model_display_text as queue_model_display_text_model
from queue_core.queue_output_probe import (
    initial_probe_path as initial_probe_path_model,
    needs_pattern_refresh as needs_pattern_refresh_model,
    path_exists_nonempty as path_exists_nonempty_model,
)
from flows.queue_output_resolution_flow import (
    maybe_refresh_probe_path as maybe_refresh_probe_path_model,
    probe_pattern_resolved as probe_pattern_resolved_model,
)
from queue_core.queue_refresh_defer import should_defer_queue_refresh as should_defer_queue_refresh_model
from queue_core.queue_start_control import (
    blocked_start_title as blocked_start_title_model,
    should_set_selected_rerun_status as should_set_selected_rerun_status_model,
    start_queue_runnable_state as start_queue_runnable_state_model,
)
from flows.queue_start_flow import (
    evaluate_job_start_preflight as evaluate_job_start_preflight_model,
    start_queue_mode as start_queue_mode_model,
)
from queue_core.queue_state_coordinator import QueueStateCoordinator
from render_core.render_environment_builder import (
    apply_device_env as apply_device_env_model,
    apply_retained_usd_env as apply_retained_usd_env_model,
    available_gpu_ids as available_gpu_ids_model,
    base_render_environment as base_render_environment_model,
    parse_device_selection as parse_device_selection_model,
    should_delete_existing_retained_usd as should_delete_existing_retained_usd_model,
    should_reuse_existing_usd as should_reuse_existing_usd_model,
)
from ui_core.ui_state_rules import build_ui_state as build_ui_state_model
from ui_core.window_layout_coordinator import WindowLayoutCoordinator
from ui_core.panel_splitter_coordinator import PanelSplitterCoordinator
from queue_core.queue_undo_redo import pop_history_for_shortcut as pop_history_for_shortcut_model
from ui_core.theme_support import (
    DEFAULT_THEME,
    build_app_stylesheet,
    ensure_theme_icons,
    normalize_theme_colors,
)
from ui_core.widgets import AddJobPanel, CleanStepSpinBox, JobPropertiesPanel, PanelFrame, PreferencesDialog, QueueTableItemDelegate, QueueTableWidget, RopListWidget
from worker_core.worker_client import RenderWorkerClient, ScanWorkerClient


APP_NAME = "Houdini Simple Render Manager"
ORG_NAME = "LocalOnly"
CONFIG_DIR_NAME = "HoudiniSimpleRenderManager"
CONFIG_FILE_NAME = "config.json"
THEME_FILE_NAME = "theme.json"
HOUDINI_SCRIPTS_DIR_NAME = "houdini_scripts"
LOGGER = logging.getLogger(__name__)
UI_DEFERRED_NOW_MS = 0
UI_DEFERRED_SETTLE_MS = 120
TABLER_NOTIFICATION_ICONS_DIR = Path(__file__).resolve().parent / "assets" / "third_party" / "tabler"
TABLER_NOTIFICATION_ICON_PATHS = {
    "info": TABLER_NOTIFICATION_ICONS_DIR / "info-circle.svg",
    "warning": TABLER_NOTIFICATION_ICONS_DIR / "alert-triangle.svg",
    "error": TABLER_NOTIFICATION_ICONS_DIR / "alert-octagon.svg",
}

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
        self.setObjectName("mainWindow")
        self.setAttribute(QtCore.Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setAutoFillBackground(True)
        self.setUpdatesEnabled(False)
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(self._build_app_icon())
        self.resize(1280, 820)
        self.setAcceptDrops(True)

        self.config = ConfigStore()
        self.theme = self.config.load_theme()
        self._hbatch_path = ""
        self.jobs: list[RenderJob] = []
        self._tree_rop_records_by_hip: dict[str, list[dict[str, Any]]] = {}
        self._last_queue_job_id_order: list[str] = []
        self._notification_icon_cache: dict[str, QtGui.QIcon] = {}

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
        self._panel_width_reconcile_timer = QtCore.QTimer(self)
        self._panel_width_reconcile_timer.setSingleShot(True)
        self._panel_width_reconcile_timer.timeout.connect(self._reconcile_panel_widths)
        self._pending_job_row_refresh_ids: set[str] = set()
        self._pending_job_row_refresh_timer = QtCore.QTimer(self)
        self._pending_job_row_refresh_timer.setSingleShot(True)
        self._pending_job_row_refresh_timer.timeout.connect(self._flush_pending_job_row_refreshes)
        self._last_job_status_by_id: dict[str, JobStatus] = {}
        self._pending_queue_filter_selection_ids: list[str] = []
        self._queue_filter_timer = QtCore.QTimer(self)
        self._queue_filter_timer.setSingleShot(True)
        self._queue_filter_timer.timeout.connect(self._apply_queue_filters)
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
        self._main_splitter_handle_drag_active = False
        self._applying_main_splitter_width = False
        self._queue_properties_handle_drag_active = False
        self._startup_layout_pending = True
        self._startup_layout_finalize_scheduled = False
        self._applying_panel_collapse_layout = False
        self._panel_restore_sizes: dict[tuple[int, int], int] = {}
        self._left_stack_splitter_sizes_pref: list[int] | None = None
        self._left_stack_sizes_initialized = False
        self._left_stack_user_resized_this_session = False
        self._left_stack_scrollbar_visibility_cached: bool | None = None
        self._left_pane_min_width_floor: int | None = None
        self._left_pane_content_width_floor: int | None = None
        self._left_pane_required_width_cached: int | None = None
        self._applying_job_properties_splitter = False
        self._left_notifications_height_pref: int | None = None
        self._last_recovery_headline = ""
        self._job_properties_last_state: dict[str, Any] | None = None
        self.layout_coordinator = WindowLayoutCoordinator(self)
        self.panel_splitters = PanelSplitterCoordinator(self)
        self.queue_view_state = QueueViewStateCoordinator(self)
        self.queue_context_menu = QueueContextMenuCoordinator(self)
        self.queue_refresh = QueueRefreshCoordinator(self)
        self.queue_tree_context_menu = QueueTreeContextMenuCoordinator(self)
        self.queue_state = QueueStateCoordinator(self)
        self.tree_scan = TreeScanCoordinator(self)
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
        central.setObjectName("mainWindowCentral")
        central.setAttribute(QtCore.Qt.WidgetAttribute.WA_StyledBackground, True)
        central.setAutoFillBackground(True)
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.main_splitter.addWidget(self._build_left_panel())
        self.main_splitter.addWidget(self._build_right_panel())
        self.main_splitter.setStretchFactor(0, 0)
        self.main_splitter.setStretchFactor(1, 1)
        main_handle = self.main_splitter.handle(1)
        if main_handle is not None:
            main_handle.installEventFilter(self)
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
        saved_left_pane_min_width = self.config.get("left_pane_min_width")
        try:
            self._left_pane_min_width_floor = (
                int(saved_left_pane_min_width) if saved_left_pane_min_width is not None else None
            )
            if self._left_pane_min_width_floor is not None and self._left_pane_min_width_floor <= 0:
                self._left_pane_min_width_floor = None
        except (TypeError, ValueError):
            self._left_pane_min_width_floor = None
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
        saved_left_stack_sizes = self.config.get("left_stack_splitter_sizes")
        parsed_left_stack_sizes: list[int] | None = None
        if isinstance(saved_left_stack_sizes, list):
            try:
                parsed = [max(0, int(v)) for v in saved_left_stack_sizes]
                if len(parsed) == 4:
                    parsed_left_stack_sizes = parsed
            except Exception:
                parsed_left_stack_sizes = None
        self._left_stack_splitter_sizes_pref = parsed_left_stack_sizes

    def safe_message(self, title: str, text: str, details: str | None = None) -> None:
        safe_message(self, title, text, details)

    def _schedule_deferred(self, callable_obj: Any, delay_ms: int) -> None:
        QtCore.QTimer.singleShot(int(delay_ms), callable_obj)

    def _schedule_now_and_settled(self, callable_obj: Any) -> None:
        self._schedule_deferred(callable_obj, UI_DEFERRED_NOW_MS)
        self._schedule_deferred(callable_obj, UI_DEFERRED_SETTLE_MS)

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

    def _capture_main_splitter_left_width_pref(self) -> None:
        width = self._current_main_splitter_left_width()
        if width is None or width <= 0:
            return
        self._main_splitter_left_width_pref = int(width)

    def _main_splitter_user_drag_active(self) -> bool:
        return bool(self._main_splitter_handle_drag_active)

    def eventFilter(self, watched: QtCore.QObject, event: QtCore.QEvent) -> bool:
        main_splitter = getattr(self, "main_splitter", None)
        main_handle = main_splitter.handle(1) if main_splitter is not None else None
        queue_splitter = getattr(self, "queue_properties_splitter", None)
        queue_handle = queue_splitter.handle(1) if queue_splitter is not None else None
        if watched is main_handle:
            event_type = event.type()
            if event_type == QtCore.QEvent.Type.MouseButtonPress:
                mouse_event = event
                if isinstance(mouse_event, QtGui.QMouseEvent) and mouse_event.button() == QtCore.Qt.MouseButton.LeftButton:
                    self._main_splitter_handle_drag_active = True
                    self.layout_coordinator.unlock_main_splitter_left_width_lock()
            elif event_type in {
                QtCore.QEvent.Type.MouseButtonRelease,
                QtCore.QEvent.Type.Hide,
            }:
                self._main_splitter_handle_drag_active = False
                self._capture_main_splitter_left_width_pref()
                self.layout_coordinator.sync_main_splitter_left_width_lock()
        elif watched is queue_handle:
            event_type = event.type()
            if event_type == QtCore.QEvent.Type.MouseButtonPress:
                mouse_event = event
                if isinstance(mouse_event, QtGui.QMouseEvent) and mouse_event.button() == QtCore.Qt.MouseButton.LeftButton:
                    self._queue_properties_handle_drag_active = True
                    self.layout_coordinator.unlock_job_properties_minimum_width_lock()
            elif event_type in {
                QtCore.QEvent.Type.MouseButtonRelease,
                QtCore.QEvent.Type.Hide,
            }:
                self._queue_properties_handle_drag_active = False
                self.layout_coordinator.sync_job_properties_minimum_width_lock()
        return super().eventFilter(watched, event)

    def _left_stack_content_margin(self) -> int:
        return self.layout_coordinator.left_stack_content_margin()

    def _left_stack_scrollbar_gap(self) -> int:
        return self.layout_coordinator.left_stack_scrollbar_gap()

    def _apply_startup_minimum_panel_widths(self) -> None:
        self.layout_coordinator.apply_startup_minimum_panel_widths()

    def _panel_width_batch_widgets(self, *, include_window: bool = False) -> list[QtWidgets.QWidget]:
        widgets: list[QtWidgets.QWidget] = []
        if include_window:
            widgets.append(self)
        for widget in (
            self.centralWidget(),
            getattr(self, "main_splitter", None),
            getattr(self, "left_stack_scroll", None),
            getattr(self, "queue_properties_splitter", None),
            getattr(self, "right_vertical_splitter", None),
        ):
            if isinstance(widget, QtWidgets.QWidget):
                widgets.append(widget)
        return widgets

    @staticmethod
    def _run_with_updates_suppressed(
        widgets: list[QtWidgets.QWidget],
        action: Callable[[], None],
    ) -> None:
        for widget in widgets:
            widget.setUpdatesEnabled(False)
        try:
            action()
        finally:
            for widget in reversed(widgets):
                widget.setUpdatesEnabled(True)
                widget.update()

    def _finalize_startup_layout(self) -> None:
        if not self._startup_layout_pending:
            return
        self._startup_layout_finalize_scheduled = False
        self._panel_width_reconcile_timer.stop()
        def _finalize() -> None:
            self._update_left_panel_expanded_min_heights()
            self._sync_left_stack_scrollbar_compensation()
            self._capture_left_pane_min_width_floor()
            self._apply_startup_minimum_panel_widths()
            self._apply_main_splitter_left_width_pref()
            self._apply_left_stack_splitter_default_sizes()
            self._capture_main_splitter_left_width_pref()
            self._reconcile_panel_widths()
            self._startup_layout_pending = False
        self._run_with_updates_suppressed(self._panel_width_batch_widgets(include_window=True), _finalize)
        self.update()

    def _schedule_panel_width_reconcile(self, delay_ms: int = 0) -> None:
        self.layout_coordinator.schedule_panel_width_reconcile(delay_ms)

    def _reconcile_panel_widths(self) -> None:
        self._run_with_updates_suppressed(
            self._panel_width_batch_widgets(),
            self.layout_coordinator.reconcile_panel_widths,
        )

    def _apply_main_splitter_left_minimum_width(self) -> None:
        self.layout_coordinator.apply_main_splitter_left_minimum_width()

    def _job_properties_min_width(self) -> int:
        return self.layout_coordinator.job_properties_min_width()

    def _job_properties_collapse_threshold(self) -> int:
        return self.layout_coordinator.job_properties_collapse_threshold()

    def _job_properties_target_width(self, *, current_width: int, total_width: int) -> int:
        return self.layout_coordinator.job_properties_target_width(
            current_width=current_width,
            total_width=total_width,
        )

    def _set_queue_properties_sizes(self, left_width: int, right_width: int) -> None:
        self.layout_coordinator.set_queue_properties_sizes(left_width, right_width)

    def _collapse_job_properties_panel(self, *, total_width: int, remembered_width: int | None = None) -> None:
        self.layout_coordinator.collapse_job_properties_panel(
            total_width=total_width,
            remembered_width=remembered_width,
        )

    def _restore_job_properties_panel(self, *, total_width: int, fallback_width: int | None = None) -> None:
        self.layout_coordinator.restore_job_properties_panel(
            total_width=total_width,
            fallback_width=fallback_width,
        )

    def _apply_job_properties_minimum_width(self) -> None:
        self.layout_coordinator.apply_job_properties_minimum_width()

    def _compute_left_pane_content_required_width(self) -> int | None:
        return self.layout_coordinator.compute_left_pane_content_required_width()

    def _compute_left_pane_required_width(self) -> int | None:
        return self.layout_coordinator.compute_left_pane_required_width()

    def _enforce_left_pane_min_width_floor(self) -> None:
        self.layout_coordinator.enforce_left_pane_min_width_floor()

    def _capture_left_pane_min_width_floor(self) -> None:
        self.layout_coordinator.capture_left_pane_min_width_floor()

    def _left_stack_scrollbar_visible(self) -> bool:
        return self.layout_coordinator.left_stack_scrollbar_visible()

    def _sync_left_stack_scrollbar_compensation(self) -> None:
        self.layout_coordinator.sync_left_stack_scrollbar_compensation()

    def _on_left_stack_scroll_metrics_changed(self) -> None:
        self.layout_coordinator.on_left_stack_scroll_metrics_changed()

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
        self.panel_splitters.apply_left_splitter_default_sizes()

    def _apply_left_column_splitter_default_sizes(self) -> None:
        self.panel_splitters.apply_left_column_splitter_default_sizes()

    def _apply_left_stack_splitter_default_sizes(self) -> None:
        self.panel_splitters.apply_left_stack_splitter_default_sizes()

    def _update_create_job_panel_height_cap(self) -> None:
        self.panel_splitters.update_create_job_panel_height_cap()

    def _update_left_panel_expanded_min_heights(self) -> None:
        self.panel_splitters.update_left_panel_expanded_min_heights()

    def _apply_left_stack_panel_heights(self, panel_pref: list[int] | tuple[int, int, int]) -> None:
        self.panel_splitters.apply_left_stack_panel_heights(panel_pref)

    def _register_collapsible_panel(
        self,
        panel: QtWidgets.QWidget | None,
        splitter: QtWidgets.QSplitter | None,
        index: int,
    ) -> None:
        self.panel_splitters.register_collapsible_panel(panel, splitter, index)

    def _on_panel_expanded_changed(
        self,
        panel: PanelFrame,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
    ) -> None:
        self.panel_splitters.on_panel_expanded_changed(panel, splitter, index, expanded)

    def _on_left_stack_panel_expanded_changed(self, panel: PanelFrame, index: int, expanded: bool) -> None:
        self.panel_splitters.on_left_stack_panel_expanded_changed(panel, index, expanded)

    def _current_main_splitter_left_width(self) -> int | None:
        return self.panel_splitters.current_main_splitter_left_width()

    def _restore_main_splitter_left_width_deferred(self, left_width: int) -> None:
        self.panel_splitters.restore_main_splitter_left_width_deferred(left_width)

    def _restore_main_splitter_left_width(self, left_width: int) -> None:
        self.panel_splitters.restore_main_splitter_left_width(left_width)

    def _pack_left_column_top(self) -> None:
        self.panel_splitters.pack_left_column_top()

    def _apply_splitter_sizes(self, splitter: QtWidgets.QSplitter, sizes: list[int] | tuple[int, ...]) -> None:
        self.panel_splitters.apply_splitter_sizes(splitter, sizes)

    def _handle_right_queue_logs_special_toggle(
        self,
        *,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
        total: int,
        top_widget: PanelFrame,
        bottom_widget: PanelFrame,
    ) -> bool:
        return self.panel_splitters._handle_right_queue_logs_special_toggle(
            splitter=splitter,
            index=index,
            expanded=expanded,
            total=total,
            top_widget=top_widget,
            bottom_widget=bottom_widget,
        )

    def _rebalance_splitter_for_panel_toggle(
        self,
        *,
        panel: PanelFrame,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
    ) -> None:
        self.panel_splitters.rebalance_splitter_for_panel_toggle(
            panel=panel,
            splitter=splitter,
            index=index,
            expanded=expanded,
        )

    def _on_left_splitter_moved(self, _pos: int, _index: int) -> None:
        self.panel_splitters.on_left_splitter_moved()

    def _on_left_column_splitter_moved(self, _pos: int, _index: int) -> None:
        self.panel_splitters.on_left_column_splitter_moved()

    def _on_left_stack_splitter_moved(self, _pos: int, _index: int) -> None:
        self.panel_splitters.on_left_stack_splitter_moved()

    def _on_right_splitter_moved(self, _pos: int, _index: int) -> None:
        self.panel_splitters.on_right_splitter_moved()

    def _maybe_auto_collapse_splitter_panel(
        self,
        *,
        splitter: QtWidgets.QSplitter,
        sizes: list[int] | tuple[int, ...],
        index: int,
    ) -> bool:
        return self.panel_splitters.maybe_auto_collapse_splitter_panel(
            splitter=splitter,
            sizes=sizes,
            index=index,
        )

    def _maintain_left_stack_top_pack(self) -> None:
        self.panel_splitters.maintain_left_stack_top_pack()

    def _auto_collapse_left_stack_panels_from_sizes(self, sizes: list[int] | tuple[int, ...]) -> bool:
        return self.panel_splitters.auto_collapse_left_stack_panels_from_sizes(sizes)

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
        if not self._main_splitter_user_drag_active():
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
            self.layout_coordinator.sync_main_splitter_left_width_lock()
            return
        if self._main_splitter_left_width_pref is None:
            if sizes[0] > 0:
                self._main_splitter_left_width_pref = int(sizes[0])
            self.layout_coordinator.sync_main_splitter_left_width_lock()
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
        self.layout_coordinator.sync_main_splitter_left_width_lock()

    def _build_job_create_panel(self) -> QtWidgets.QWidget:
        self.add_job_panel = AddJobPanel(self.config)
        self.add_job_panel.setObjectName("panelEmbeddedGroup")
        self.add_job_panel.setTitle("")
        self.add_job_panel.add_job_requested.connect(self._handle_add_job_requested)
        self.add_job_panel.scan_requested.connect(self._handle_scan_requested)
        self.create_job_frame = PanelFrame("Create Job", self.add_job_panel, collapsible=True)
        return self.create_job_frame

    def _build_left_panel(self) -> QtWidgets.QWidget:
        host = QtWidgets.QWidget()
        host.setAttribute(QtCore.Qt.WidgetAttribute.WA_StyledBackground, True)
        host.setAutoFillBackground(True)
        self.left_panel_host = host
        layout = QtWidgets.QVBoxLayout(host)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.left_stack_host = QtWidgets.QWidget()
        self.left_stack_host.setObjectName("transparentHost")
        self.left_stack_layout = QtWidgets.QVBoxLayout(self.left_stack_host)
        content_margin = self._left_stack_content_margin()
        self.left_stack_layout.setContentsMargins(content_margin, 0, content_margin, 0)
        self.left_stack_layout.setSpacing(6)
        self.left_stack_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        self.left_stack_layout.addWidget(self._build_job_create_panel())
        self.left_stack_layout.addWidget(self._build_tree_view_panel())
        self.left_stack_layout.addWidget(self._build_notifications_panel(), 1)

        self.left_stack_scroll = QtWidgets.QScrollArea()
        self.left_stack_scroll.setObjectName("leftPaneScroll")
        self.left_stack_scroll.setWidgetResizable(True)
        self.left_stack_scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.left_stack_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.left_stack_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.left_stack_scroll.setWidget(self.left_stack_host)
        left_vbar = self.left_stack_scroll.verticalScrollBar()
        if left_vbar is not None:
            left_vbar.rangeChanged.connect(lambda *_args: self._on_left_stack_scroll_metrics_changed())
        self.create_job_frame.expanded_changed.connect(
            lambda expanded, p=self.create_job_frame, i=0: self._on_left_stack_panel_expanded_changed(p, i, expanded)
        )
        self.tree_view_frame.expanded_changed.connect(
            lambda expanded, p=self.tree_view_frame, i=1: self._on_left_stack_panel_expanded_changed(p, i, expanded)
        )
        self.notifications_frame.expanded_changed.connect(
            lambda expanded, p=self.notifications_frame, i=2: self._on_left_stack_panel_expanded_changed(p, i, expanded)
        )
        compact_panel_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Maximum)
        self.create_job_frame.setSizePolicy(compact_panel_policy)
        self.create_job_frame.set_expanded_size_policy(compact_panel_policy)
        self.tree_view_frame.setSizePolicy(compact_panel_policy)
        self.tree_view_frame.set_expanded_size_policy(compact_panel_policy)
        self._update_left_panel_expanded_min_heights()
        fill_panel_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Expanding)
        self.notifications_frame.setSizePolicy(fill_panel_policy)
        self.notifications_frame.set_expanded_size_policy(fill_panel_policy)
        self.notifications_frame.set_keep_expanding_when_collapsed(False)
        layout.addWidget(self.left_stack_scroll, 1)
        self._sync_left_stack_scrollbar_compensation()
        self._capture_left_pane_min_width_floor()
        return host

    def _build_notifications_panel(self) -> QtWidgets.QWidget:
        box = QtWidgets.QGroupBox("Notifications")
        layout = QtWidgets.QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        header_row = QtWidgets.QHBoxLayout()
        header_row.setContentsMargins(8, 8, 8, 8)
        header_row.setSpacing(8)
        self.btn_clear_notifications = QtWidgets.QPushButton("Clear")
        self.btn_clear_notifications.clicked.connect(self._clear_notifications_view_only)
        header_row.addWidget(self.btn_clear_notifications)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        self.notifications_list = RopListWidget()
        self.notifications_list.setObjectName("notificationsList")
        self.notifications_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.notifications_list.setAlternatingRowColors(True)
        self.notifications_list.setUniformItemSizes(False)
        self.notifications_list.setWordWrap(True)
        self.notifications_list.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.notifications_list.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        self.notifications_list.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.notifications_list.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.notifications_list.verticalScrollBar().setSingleStep(20)
        layout.addWidget(self.notifications_list, 1)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        self.notifications_frame = PanelFrame("Notifications", box, collapsible=True)
        self.notifications_frame.set_body_margins(0, 0, 0, 0)
        return self.notifications_frame

    def _build_tree_view_panel(self) -> QtWidgets.QWidget:
        panel, self.queue_tree, self.queue_tree_model, self.btn_reload_all_tree, self.chk_tree_show_used_only = build_queue_tree_panel_model(
            self,
            item_changed_handler=self._on_queue_tree_item_changed,
        )
        self.btn_reload_all_tree.clicked.connect(self._reload_all_jobs_from_files)
        raw_show_used_only = self.config.get("tree_show_used_only", True)
        show_used_only = bool(raw_show_used_only)
        if isinstance(raw_show_used_only, str):
            show_used_only = raw_show_used_only.strip().lower() not in {"0", "false", "off", "no"}
        self.chk_tree_show_used_only.setChecked(show_used_only)
        self.chk_tree_show_used_only.toggled.connect(self._on_tree_show_used_only_toggled)
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

        self.queue_table_host = QtWidgets.QWidget()
        self.queue_table_layout = QtWidgets.QVBoxLayout(self.queue_table_host)
        self.queue_table_layout.setContentsMargins(0, 0, 0, 0)
        self.queue_table_layout.setSpacing(0)
        self.queue_table_layout.addWidget(self.queue_table)

        queue_left_host = QtWidgets.QWidget()
        queue_left_host.setMinimumWidth(0)
        queue_left_layout = QtWidgets.QVBoxLayout(queue_left_host)
        queue_left_layout.setContentsMargins(0, 0, 0, 0)
        queue_left_layout.setSpacing(0)
        queue_left_layout.addWidget(controls_host)
        queue_left_layout.addWidget(self.queue_table_host, 1)

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
        self.job_properties_frame = PanelFrame("Job Properties", self.job_properties_panel, scrollable_body=True)
        self.job_properties_frame.setObjectName("jobPropertiesFrame")
        self.job_properties_frame.set_body_margins(0, 0, 0, 0)
        self.job_properties_panel.setMinimumWidth(0)
        self.job_properties_frame.setMinimumWidth(0)
        job_properties_min_width = int(self._job_properties_min_width())

        self.queue_properties_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.queue_properties_splitter.addWidget(queue_left_host)
        self.queue_properties_splitter.addWidget(self.job_properties_frame)
        self.queue_properties_splitter.setChildrenCollapsible(True)
        self.queue_properties_splitter.setCollapsible(0, False)
        self.queue_properties_splitter.setCollapsible(1, True)
        self.queue_properties_splitter.setStretchFactor(0, 5)
        self.queue_properties_splitter.setStretchFactor(1, 2)
        self.queue_properties_splitter.setSizes([940, job_properties_min_width])
        self._job_properties_last_width = int(job_properties_min_width)
        queue_handle = self.queue_properties_splitter.handle(1)
        if queue_handle is not None:
            queue_handle.installEventFilter(self)
        self.queue_properties_splitter.splitterMoved.connect(self._on_queue_properties_splitter_moved)
        layout.addWidget(self.queue_properties_splitter, 1)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        panel = PanelFrame("Render Queue", box, collapsible=True)
        panel.set_body_margins(0, 0, 0, 0)
        return panel

    def _toggle_job_properties_panel(self) -> None:
        focus_widget = QtWidgets.QApplication.focusWidget()
        if isinstance(focus_widget, (QtWidgets.QLineEdit, QtWidgets.QPlainTextEdit, QtWidgets.QTextEdit, QtWidgets.QAbstractSpinBox)):
            return
        if isinstance(focus_widget, QtWidgets.QComboBox) and focus_widget.isEditable():
            return
        self.layout_coordinator.toggle_job_properties_panel(focus_widget)

    def _on_queue_properties_splitter_moved(self, _pos: int, _index: int) -> None:
        self.layout_coordinator.on_queue_properties_splitter_moved()

    def _build_right_panel(self) -> QtWidgets.QWidget:
        host = QtWidgets.QWidget()
        host.setAttribute(QtCore.Qt.WidgetAttribute.WA_StyledBackground, True)
        host.setAutoFillBackground(True)
        layout = QtWidgets.QVBoxLayout(host)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.queue_frame = self._build_queue_panel()
        self.logs_frame = self._build_log_panel()
        self.right_vertical_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        self.right_vertical_splitter.addWidget(self.queue_frame)
        self.right_vertical_splitter.addWidget(self.logs_frame)
        self.right_vertical_splitter.setChildrenCollapsible(False)
        self.right_vertical_splitter.setCollapsible(0, False)
        self.right_vertical_splitter.setCollapsible(1, False)
        self.right_vertical_splitter.setStretchFactor(0, 4)
        self.right_vertical_splitter.setStretchFactor(1, 2)
        self.right_vertical_splitter.splitterMoved.connect(self._on_right_splitter_moved)
        self._register_collapsible_panel(self.queue_frame, self.right_vertical_splitter, 0)
        self._register_collapsible_panel(self.logs_frame, self.right_vertical_splitter, 1)
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
        return PanelFrame("Logs", box, collapsible=True, scrollable_body=True)

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
            dialog_runtime_defaults_model(
                chunking_enabled=self._default_chunking_enabled(),
                chunk_size=self._default_chunk_size(),
                retry_count=self._default_retry_count(),
                retry_delay=self._default_retry_delay(),
            ),
            dialog_experimental_flags_model(
                chunking_enabled=self._experimental_chunking_enabled(),
            ),
            dialog_device_defaults_model(
                mode=self._default_device_mode(),
                selection=self._default_device_selection(),
                retain_built_usd=self._default_retain_built_usd(),
                usd_output_directory_mode=self._default_usd_output_directory_mode(),
                usd_output_directory_custom_path=self._default_usd_output_directory_custom_path(),
            ),
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
        parsed = parse_preferences_payload_model(payload)
        hbatch_path = str(parsed.get("hbatch_path", "") or "").strip()
        player_path = str(parsed.get("player_path", "") or "").strip()
        theme = parsed.get("theme")
        runtime_defaults = parsed.get("runtime_defaults")
        device_defaults = parsed.get("device_defaults")
        experimental_chunking_enabled = parsed.get("experimental_chunking_enabled")
        self._hbatch_path = hbatch_path
        self._save_hbatch_path()
        self.config.set("player_path", player_path)
        if theme is not None:
            self.theme = theme
            self.config.save_theme(self.theme)
            self._apply_theme()
        if experimental_chunking_enabled is not None:
            self.config.set("experimental_chunking_enabled", bool(experimental_chunking_enabled))
        if runtime_defaults is not None:
            chunking_enabled, chunk_size, retry_count, retry_delay = runtime_defaults
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
        if device_defaults is not None:
            mode, selection, retain_built_usd, usd_output_directory_mode, usd_output_directory_custom_path = (
                device_defaults
            )
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
        return self.queue_state.load_queue_from_path(path)

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
        if hasattr(self, "left_stack_splitter"):
            self.left_stack_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "right_vertical_splitter"):
            self.right_vertical_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "queue_properties_splitter"):
            self.queue_properties_splitter.setHandleWidth(panel_gap)
        if hasattr(self, "queue_table"):
            self.queue_table.selection_line_color = QtGui.QColor(t["selection_line"])
            self.queue_table.selection_row_color = QtGui.QColor(t["selection_row"])
            self.queue_table.selection_row_alt_color = QtGui.QColor(t["selection_row_alt"])
            self.queue_table.selection_overlay_opacity = int(t.get("selection_overlay_opacity", 95))
            self.queue_table.path_sync_overlay_opacity = int(t.get("path_sync_overlay_opacity", 28))
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
        if self._startup_layout_pending:
            return
        self._schedule_panel_width_reconcile()

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
        cpu_selected, selected_gpu_ids = parse_device_selection_model(selection)
        all_gpu_ids = available_gpu_ids_model(self._available_render_devices())
        single_process_render = self._single_process_render_enabled_for_job(job)
        retain_usd_enabled = bool(job.spec.retain_built_usd) and single_process_render
        env = base_render_environment_model(
            mode=mode,
            selection=selection,
            cpu_selected=cpu_selected,
            single_process_render=single_process_render,
            retain_usd_enabled=retain_usd_enabled,
            retained_usd_helper_path=self._retained_usd_helper_path(),
        )
        if retain_usd_enabled:
            planned_build_range = self._current_retained_usd_build_range(job)
            output_path = str(job.runtime.retained_usd_path or "").strip()
            configured_output_dir = self._configured_retained_usd_folder_preview(job)
            invalid_reason = self._retained_usd_invalid_reason(job)
            should_delete_existing = should_delete_existing_retained_usd_model(
                output_path=output_path,
                reuse_retained_usd=bool(job.spec.reuse_retained_usd),
                invalid_reason=invalid_reason,
            )
            if should_delete_existing:
                self._delete_retained_usd_folder_for_job(job)
                self._sync_retained_usd_file_state(job)
                if planned_build_range is not None:
                    job.runtime.retained_usd_build_start_frame = int(planned_build_range[0])
                    job.runtime.retained_usd_build_end_frame = int(planned_build_range[1])
                    job.runtime.retained_usd_build_step = int(planned_build_range[2])
                output_path = str(job.runtime.retained_usd_path or "").strip()
            reuse_existing = should_reuse_existing_usd_model(
                reuse_retained_usd=bool(job.spec.reuse_retained_usd),
                output_path=output_path,
                retained_reusable=bool(job.runtime.retained_usd_reusable),
                invalid_reason=invalid_reason,
            )
            job.runtime.retained_usd_metadata_pending_write = bool(not reuse_existing)
            apply_retained_usd_env_model(
                env,
                output_path=output_path,
                configured_output_dir=configured_output_dir,
                reuse_existing=reuse_existing,
            )
            if reuse_existing:
                stale_reason = self._retained_usd_stale_reason(job)
                if stale_reason:
                    self._append_log("Stderr", f"[RetainUSD] {stale_reason}\n")
                    self._append_notification_message(stale_reason, "warning")
            elif invalid_reason:
                self._append_log("Stderr", f"[RetainUSD] {invalid_reason}\n")
                self._append_notification_message(invalid_reason, "warning")
        apply_device_env_model(
            env,
            mode=mode,
            all_gpu_ids=all_gpu_ids,
            selected_gpu_ids=selected_gpu_ids,
            cpu_selected=cpu_selected,
        )
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
        return job_file_name_from_path_model(job.spec.hip_path)

    @staticmethod
    def _job_rop_name(job: RenderJob) -> str:
        return job_rop_name_from_path_model(job.spec.rop_path)

    @staticmethod
    def _safe_usd_folder_name(name: str) -> str:
        return safe_usd_folder_name_model(name)

    def _effective_usd_output_directory_mode_for_job(self, job: RenderJob) -> UsdOutputDirectoryMode:
        return UsdOutputDirectoryMode.coerce(job.spec.usd_output_directory_mode)

    def _effective_usd_output_directory_custom_path_for_job(self, job: RenderJob) -> str:
        return str(job.spec.usd_output_directory_custom_path or "").strip()

    @staticmethod
    def _single_process_render_enabled_for_job(job: RenderJob) -> bool:
        return bool(job.spec.render_all_frames_single_process)

    def _configured_retained_usd_folder_preview(self, job: RenderJob) -> str:
        mode = self._effective_usd_output_directory_mode_for_job(job)
        return configured_retained_usd_folder_preview_model(
            hip_path=str(job.spec.hip_path or ""),
            rop_path=str(job.spec.rop_path or ""),
            mode=mode,
            custom_path=self._effective_usd_output_directory_custom_path_for_job(job),
        )

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
        next_state = build_job_properties_state_for_selection_model(
            selected_jobs=selected_jobs,
            panel_default_state=self._job_properties_panel_default_state,
            mixed_value=self._mixed_value,
            job_file_name=self._job_file_name,
            job_rop_name=self._job_rop_name,
            single_job_retained_state=self._single_job_retained_usd_panel_state,
            selected_retained_paths=self._selected_retained_usd_paths,
            can_edit_job_for_panel=lambda job: can_edit_job(
                job,
                is_active_job=self._is_active_job(job),
                is_locked=self._is_job_path_sync_locked(job),
            ).allowed,
            device_option_states_for_jobs=lambda jobs, show_custom_devices, editable: self._device_option_states_for_jobs(
                jobs,
                show_custom_devices=show_custom_devices,
                editable=editable,
            ),
            is_active_job=self._is_active_job,
            is_locked_job=self._is_job_path_sync_locked,
            unchecked_state=unchecked_state,
            checked_state=checked_state,
            partial_state=partial_state,
            default_device_mode=DeviceOverrideMode.DEFAULT.value,
            default_usd_output_mode=UsdOutputDirectoryMode.DEFAULT_TEMP.value,
        )
        last_state = getattr(self, "_job_properties_last_state", None)
        if last_state == next_state:
            return
        panel.set_state(next_state)
        self._job_properties_last_state = next_state

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
        self._pending_queue_filter_selection_ids = self._selected_job_ids()
        self._queue_filter_timer.start(180)

    def _apply_queue_filters(self) -> None:
        proxy = self._queue_proxy_model()
        if proxy is None:
            return
        selected_ids = list(self._pending_queue_filter_selection_ids)
        self._pending_queue_filter_selection_ids = []
        proxy.set_search_text(self.queue_search_edit.text() if hasattr(self, "queue_search_edit") else "")
        current_value = ""
        if hasattr(self, "queue_status_filter") and self.queue_status_filter is not None:
            current_value = str(self.queue_status_filter.currentData() or "")
        proxy.set_status_filter(current_value)
        proxy.set_enabled_only(False)
        if selected_ids:
            selected_id_set = set(selected_ids)
            selection_model = self.queue_table.selectionModel()
            if selection_model is not None:
                blocker = QtCore.QSignalBlocker(selection_model)
                try:
                    self.queue_table.clearSelection()
                    first_idx = QtCore.QModelIndex()
                    for row, job in enumerate(self.jobs):
                        if job.id not in selected_id_set:
                            continue
                        model_idx = self._queue_view_index_from_source_row(row, 0)
                        if not model_idx.isValid():
                            continue
                        selection_model.select(
                            model_idx,
                            QtCore.QItemSelectionModel.SelectionFlag.Select
                            | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                        )
                        if not first_idx.isValid():
                            first_idx = model_idx
                    if first_idx.isValid():
                        selection_model.setCurrentIndex(first_idx, QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)
                finally:
                    del blocker
            self._on_queue_selection_changed()
            return
        self._update_job_properties_panel()
        self._refresh_ui_state()

    def _flush_pending_queue_refresh(self) -> None:
        self.queue_refresh.flush_pending_queue_refresh()

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
        self.queue_refresh.save_and_refresh_queue(
            select_job_id=select_job_id,
            select_job_ids=select_job_ids,
            select_row=select_row,
        )

    def _mark_job_offline(self, job: RenderJob, reason: str | None = None) -> None:
        mark_job_offline_model(job, reason)

    def _restore_job_online_status(self, job: RenderJob) -> None:
        restore_job_online_status_model(job)

    def _selection_ids_for_refresh(self, fallback_job_ids: list[str] | None = None) -> list[str] | None:
        return self.queue_refresh.selection_ids_for_refresh(fallback_job_ids)

    def _defer_refresh_queue_tree_view(self) -> None:
        self.tree_scan.defer_refresh_queue_tree_view()

    def _defer_save_and_refresh_queue(
        self,
        select_job_ids: list[str] | None = None,
        *,
        block_interaction: bool = False,
        status_text: str | None = None,
    ) -> None:
        self.queue_refresh.defer_save_and_refresh_queue(
            select_job_ids,
            block_interaction=block_interaction,
            status_text=status_text,
        )

    def _tree_context_target_jobs(self, index: QtCore.QModelIndex) -> list[RenderJob]:
        if not index.isValid():
            return []
        hip_path = str(index.data(TREE_HIP_ROLE) or "").strip()
        rop_path = str(index.data(TREE_ROP_ROLE) or "").strip()
        kind = str(index.data(TREE_KIND_ROLE) or "").strip().lower()
        return tree_context_target_jobs_model(
            self.jobs,
            hip_path=hip_path,
            rop_path=rop_path,
            kind=kind,
        )

    def _tree_context_reload_target_jobs(self, index: QtCore.QModelIndex, target_jobs: list[RenderJob]) -> list[RenderJob]:
        if not index.isValid():
            return list(target_jobs)
        hip_path = str(index.data(TREE_HIP_ROLE) or "").strip()
        if not hip_path:
            return list(target_jobs)
        file_jobs = [job for job in self.jobs if str(job.spec.hip_path or "").strip() == hip_path]
        return file_jobs or list(target_jobs)

    def _tree_cached_rop_record(self, hip_path: str, rop_path: str) -> dict[str, Any] | None:
        hip_value = str(hip_path or "").strip()
        rop_value = str(rop_path or "").strip()
        if not hip_value or not rop_value:
            return None
        for record in self._tree_rop_records_by_hip.get(hip_value, []):
            if str(record.get("path", "") or "").strip() == rop_value:
                return dict(record)
        return None

    def _create_job_from_tree_rop(self, *, hip_path: str, rop_path: str) -> None:
        payload = {
            "hip_path": str(hip_path or "").strip(),
            "rop_path": str(rop_path or "").strip(),
            "name": "",
            "frame_range_mode": "use_rop",
            "start_frame": None,
            "end_frame": None,
            "step": None,
        }
        try:
            job = self._build_job_from_payload(payload)
        except ValueError as exc:
            safe_message(self, "Create Job", str(exc))
            return
        except Exception as exc:
            safe_message(self, "Create Job", f"Failed to create job: {exc}", traceback.format_exc())
            return

        record = self._tree_cached_rop_record(payload["hip_path"], payload["rop_path"])
        if record is not None:
            strict_value = record.get("strict_frame_range")
            if strict_value is not None:
                job.spec.strict_frame_range = bool(strict_value)
            allframes_value = record.get("all_frames_single_process")
            if allframes_value is not None:
                job.spec.render_all_frames_single_process = bool(allframes_value)
            output_hint = str(record.get("output_path", "") or "").strip()
            if output_hint:
                job.view.out_file_sample_path = output_hint
                job.view.out_path = self._normalize_output_display_path(output_hint)
            rs = record.get("runtime_start_frame")
            re_ = record.get("runtime_end_frame")
            rstep = record.get("runtime_step")
            if rs is not None and re_ is not None:
                job.runtime.runtime_start_frame = rs
                job.runtime.runtime_end_frame = re_
                job.runtime.runtime_step = rstep
                job.runtime.rop_default_start_frame = rs
                job.runtime.rop_default_end_frame = re_
                job.runtime.rop_default_step = rstep

        previous_selection = self._selected_job_ids()
        insert_index = len(self.jobs)
        self.jobs.append(job)
        self._push_history_command(
            {
                "kind": "insert_jobs",
                "entries": [{"index": insert_index, "job": self._job_to_persisted_dict(job)}],
                "undo_select_job_ids": previous_selection,
                "redo_select_job_ids": [job.id],
            }
        )
        self._save_queue_state()
        self._refresh_queue_table(select_job_id=job.id)
        self._set_status_message(f"Added job: {job.display_name()}", 3000)

    def _show_queue_tree_context_menu(self, pos: QtCore.QPoint) -> None:
        self.queue_tree_context_menu.show_queue_tree_context_menu(pos)

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
        self._refresh_tree_rop_cache_for_hips([job.spec.hip_path for job in target_jobs])
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
        self.queue_context_menu.show_queue_context_menu(pos)

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
        probe_path = initial_probe_path_model(sample_file_path, out_path)
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
        probe_path, node_not_found = maybe_refresh_probe_path_model(
            probe_path=probe_path,
            sample_file_path=sample_file_path,
            start_frame=start_frame,
            hip_exists=Path(job.spec.hip_path).exists(),
            hbatch_exists=self._hbatch_exists(),
            hip_path=job.spec.hip_path,
            rop_path=job.spec.rop_path,
            needs_pattern_refresh_fn=needs_pattern_refresh_model,
            frame_path_for_frame_fn=self._frame_sequence_path_for_frame,
            probe_rop_info_fn=self._probe_rop_info,
            apply_rop_info_fn=lambda info: apply_rop_info_to_job_model(
                job,
                info,
                self._normalize_output_display_path,
                apply_runtime_range=False,
            ),
            refreshed_sample_path_fn=lambda: str(job.view.out_file_sample_path or ""),
        )
        if node_not_found:
            self._mark_job_offline(job, "ROP node not found in HIP file.")
            self._save_and_refresh_queue(select_job_id=job.id)
            return None

        probe_decision = validate_resume_probe_path(
            probe_path=probe_path,
            pattern_resolved=probe_pattern_resolved_model(
                probe_path=probe_path,
                start_frame=start_frame,
                frame_path_for_frame_fn=self._frame_sequence_path_for_frame,
            ),
        )
        if not probe_decision.valid:
            if interactive and probe_decision.title:
                safe_message(self, probe_decision.title, probe_decision.message)
            return None

        scan = first_missing_frame_and_contiguous_done_model(
            start_frame=start_frame,
            end_frame=end_frame,
            step=step,
            path_for_frame=lambda frame: self._frame_sequence_path_for_frame(probe_path, frame),
            exists_nonempty=path_exists_nonempty_model,
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
        probe_path = initial_probe_path_model(sample_file_path, out_path)
        probe_path, node_not_found = maybe_refresh_probe_path_model(
            probe_path=probe_path,
            sample_file_path=sample_file_path,
            start_frame=start_frame,
            hip_exists=Path(job.spec.hip_path).exists(),
            hbatch_exists=self._hbatch_exists(),
            hip_path=job.spec.hip_path,
            rop_path=job.spec.rop_path,
            needs_pattern_refresh_fn=needs_pattern_refresh_model,
            frame_path_for_frame_fn=self._frame_sequence_path_for_frame,
            probe_rop_info_fn=self._probe_rop_info,
            apply_rop_info_fn=lambda info: apply_rop_info_to_job_model(
                job,
                info,
                self._normalize_output_display_path,
                apply_runtime_range=False,
            ),
            refreshed_sample_path_fn=lambda: str(job.view.out_file_sample_path or ""),
        )
        if node_not_found:
            self._mark_job_offline(job, "ROP node not found in HIP file.")
            self._save_and_refresh_queue(select_job_id=job.id)
            return None

        probe_decision = validate_render_missing_probe_path(
            probe_path=probe_path,
            pattern_resolved=probe_pattern_resolved_model(
                probe_path=probe_path,
                start_frame=start_frame,
                frame_path_for_frame_fn=self._frame_sequence_path_for_frame,
            ),
        )
        if not probe_decision.valid:
            if interactive and probe_decision.title:
                safe_message(self, probe_decision.title, probe_decision.message)
            return None

        scan = missing_frame_runs_and_existing_count_model(
            start_frame=start_frame,
            end_frame=end_frame,
            step=step,
            path_for_frame=lambda frame: self._frame_sequence_path_for_frame(probe_path, frame),
            exists_nonempty=path_exists_nonempty_model,
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
        return self.queue_state.queue_view_to_persisted_dict()

    def queue_view_state_payload(self) -> dict[str, Any]:
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
        return self.queue_state.save_queue_state(path)

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
        self.queue_state.load_persisted_queue()

    def _selected_job(self) -> RenderJob | None:
        return selected_job_for_row_model(self.jobs, self._selected_row())

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
        self.queue_refresh.refresh_queue_table(
            select_row=select_row,
            select_job_id=select_job_id,
            select_job_ids=select_job_ids,
        )

    def _refresh_job_row(self, job_id: str) -> None:
        self.queue_refresh.refresh_job_row(job_id)

    def _flush_pending_job_row_refreshes(self) -> None:
        self.queue_refresh.flush_pending_job_row_refreshes()

    def _sync_last_job_status_snapshot(self) -> None:
        self.queue_refresh.sync_last_job_status_snapshot()

    def _refresh_queue_tree_view(self) -> None:
        self.tree_scan.refresh_queue_tree_view()

    @staticmethod
    def _sanitize_tree_rop_records(records: Any) -> list[dict[str, Any]]:
        return TreeScanCoordinator.sanitize_tree_rop_records(records)

    def _persist_tree_rop_cache(self) -> None:
        self.tree_scan.persist_tree_rop_cache()

    def _replace_tree_rop_cache_for_hip(self, hip_path: str, records: list[dict[str, Any]]) -> None:
        self.tree_scan.replace_tree_rop_cache_for_hip(hip_path, records)

    def _selected_scan_records_for_tree(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.tree_scan.selected_scan_records_for_tree(records)

    def _refresh_tree_rop_cache_for_hips(self, hip_paths: list[str]) -> None:
        self.tree_scan.refresh_tree_rop_cache_for_hips(hip_paths)

    def _tree_rop_paths_for_hip(self, hip_path: str) -> list[str]:
        return self.tree_scan.tree_rop_paths_for_hip(hip_path)

    def _on_tree_show_used_only_toggled(self, checked: bool) -> None:
        self.tree_scan.on_tree_show_used_only_toggled(checked)

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
        entries, next_signature = appendable_notifications_model(
            candidates=[(message, severity)],
            last_signature=self._last_notification_signature,
            dedupe_consecutive=True,
        )
        self._append_notification_entries(entries, next_signature=next_signature)

    def _append_notification_entries(
        self,
        entries: list[tuple[str, str]],
        *,
        next_signature: tuple[str, str] | None,
    ) -> bool:
        if not entries:
            return False
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is None:
            return False
        for text, sev in entries:
            item = QtWidgets.QListWidgetItem(text)
            item.setIcon(self._notification_icon_for_severity(sev))
            item.setForeground(QtGui.QBrush(self._notification_color_for_severity(sev)))
            list_widget.insertItem(0, item)
        self._last_notification_signature = next_signature
        self._trim_notifications_list(max_items=250)
        return True

    def _trim_notifications_list(self, *, max_items: int) -> None:
        list_widget = getattr(self, "notifications_list", None)
        if list_widget is None:
            return
        remove_count = trim_notification_count_model(count=list_widget.count(), max_items=max_items)
        for _ in range(remove_count):
            list_widget.takeItem(list_widget.count() - 1)
        if list_widget.count() > 0:
            list_widget.scrollToTop()

    def _append_notifications(self, source: str, text: str) -> None:
        entries, next_signature = appendable_notifications_for_log_model(
            source=source,
            text=text,
            last_signature=self._last_notification_signature,
            dedupe_consecutive=True,
        )
        self._append_notification_entries(entries, next_signature=next_signature)

    def _notification_messages_for_log(self, source: str, text: str) -> list[tuple[str, str]]:
        return notification_messages_for_log_model(source, text)

    @staticmethod
    def _notification_summary_for_line(source: str, line: str) -> tuple[str, str] | None:
        return notification_summary_for_line_model(source, line)

    @staticmethod
    def _classified_render_error_notification(low: str) -> tuple[str, str] | None:
        return classified_render_error_notification_model(low)

    @staticmethod
    def _load_tinted_svg_icon(path: Path, color: QtGui.QColor, *, size: int = 20) -> QtGui.QIcon:
        renderer = QtSvg.QSvgRenderer(str(path))
        if not renderer.isValid():
            return QtGui.QIcon()
        pixmap = QtGui.QPixmap(size, size)
        pixmap.fill(QtCore.Qt.GlobalColor.transparent)
        painter = QtGui.QPainter(pixmap)
        try:
            renderer.render(painter)
            painter.setCompositionMode(QtGui.QPainter.CompositionMode.CompositionMode_SourceIn)
            painter.fillRect(pixmap.rect(), color)
        finally:
            painter.end()
        return QtGui.QIcon(pixmap)

    def _notification_icon_for_severity(self, severity: str) -> QtGui.QIcon:
        sev = str(severity or "info").lower()
        cached_icon = self._notification_icon_cache.get(sev)
        if cached_icon is not None and not cached_icon.isNull():
            return cached_icon
        icon_path = TABLER_NOTIFICATION_ICON_PATHS.get(sev)
        if icon_path is not None and icon_path.exists():
            icon = self._load_tinted_svg_icon(icon_path, self._notification_color_for_severity(sev))
            if not icon.isNull():
                self._notification_icon_cache[sev] = icon
                return icon
        style = self.style()
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
        return build_diagnostics_snapshot_model(
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
        self._flush_pending_job_row_refreshes()
        self._update_job_properties_panel()
        self._refresh_ui_state()

    @staticmethod
    def _parse_percent_value(text: str) -> int | None:
        return parse_percent_value_model(text)

    def _queue_progress_split_values(self, job: RenderJob) -> tuple[int | None, int | None]:
        return queue_progress_split_values_model(job)

    def _queue_header_visual_order(self) -> list[int]:
        return self.queue_view_state.header_visual_order()

    def _queue_hidden_columns_from_data(self, raw: Any) -> set[int]:
        return self.queue_view_state.hidden_columns_from_data(raw)

    def _queue_default_column_width(self, logical: int, fallback: int) -> int:
        return self.queue_view_state.default_column_width(logical, fallback)

    def _sanitized_queue_column_width(self, logical: int, width: int) -> int:
        return self.queue_view_state.sanitized_column_width(logical, width)

    def _queue_column_widths_from_data(self, raw: Any) -> dict[int, int]:
        return self.queue_view_state.column_widths_from_data(raw)

    def _reset_queue_view_to_defaults(self) -> None:
        self.queue_view_state.reset_to_defaults()

    def _apply_queue_view_from_persisted_data(self, raw: Any) -> None:
        self.queue_view_state.apply_persisted_data(raw)

    def _is_valid_queue_header_grouping(self) -> bool:
        return self.queue_view_state.is_valid_header_grouping()

    def _restore_queue_header_order(self, visual_order: list[int]) -> None:
        self.queue_view_state.restore_header_order(visual_order)

    def _on_queue_header_section_moved(self, _logical: int, _old_visual: int, _new_visual: int) -> None:
        self.queue_view_state.on_header_section_moved()

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
        return queue_model_display_text_model(
            model,
            row,
            column,
            display_role=QtCore.Qt.ItemDataRole.DisplayRole,
        )

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
        self.tree_scan.handle_scan_worker_message(message)

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
        can_start_selected, has_runnable = start_queue_runnable_state_model(
            selected_job=selected,
            is_runnable=self._is_job_runnable,
            jobs=self.jobs,
        )
        start_decision = evaluate_start_request_model(
            self._queue_lifecycle_state(),
            hbatch_exists=self._hbatch_exists(),
            has_runnable=has_runnable,
            can_start_selected=can_start_selected,
        )
        mode = start_queue_mode_model(
            queue_active=bool(self.queue_active),
            queue_paused=bool(self.queue_paused),
            resume_existing=bool(start_decision.resume_existing),
            allowed=bool(start_decision.allowed),
        )
        if mode == "already_active":
            return
        if mode == "resume_existing":
            self._apply_queue_lifecycle_state(with_queue_resumed_model(self._queue_lifecycle_state()))
            self._append_log("Stdout", "\n[Queue] Resumed\n")
            self._set_status_message("Queue resumed", 3000)
            self._maybe_start_next_job()
            self._refresh_ui_state()
            return
        if mode == "blocked":
            reason = str(start_decision.reason or "")
            title = blocked_start_title_model(reason)
            safe_message(self, title, reason)
            return
        self._write_queue_snapshot("before_start")
        self._apply_queue_lifecycle_state(with_queue_started_model(self._queue_lifecycle_state()))
        self._append_log("Stdout", "\n=== Queue Started ===\n")
        self._set_status_message("Queue started")
        self._refresh_ui_state()
        if can_start_selected:
            if should_set_selected_rerun_status_model(selected):
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
        preflight = evaluate_job_start_preflight_model(
            hbatch_exists=self._hbatch_exists(),
            hip_exists=Path(job.spec.hip_path).exists(),
        )
        if not preflight.allowed:
            if preflight.dialog_title and preflight.dialog_message:
                safe_message(self, preflight.dialog_title, preflight.dialog_message)
            if preflight.abort_queue:
                self._finish_queue("Queue aborted")
                return
            if preflight.offline_reason:
                self._mark_job_offline(job, preflight.offline_reason)
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
        return current_job_by_id_model(self.jobs, str(self.current_job_id or ""))

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
        path = selected_job_log_path_model(self._selected_job())
        if path is None:
            return
        if not path.exists():
            safe_message(self, "Log Missing", f"Log file does not exist:\n{path}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(path)))

    def _job_preview_path(self, job: RenderJob) -> Path | None:
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        return resolve_job_preview_path_model(
            candidate=str(job.view.out_file_sample_path or ""),
            resolved_range=resolved,
            frame_path_for_frame=self._frame_sequence_path_for_frame,
        )

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

        log_paths = discover_log_files_model(logs_dir)
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

        deleted, failed = delete_log_files_model(log_paths)
        title, message, details = log_deletion_feedback_model(deleted=deleted, failed=failed)
        safe_message(self, title, message, details)

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
        render_job_active = self._render_job_active()
        scan_in_progress = self._scan_in_progress()
        create_job_scan_in_progress = bool(self._create_job_scan_in_progress)
        hbatch_ok = self._hbatch_exists()
        path_sync_in_progress = bool(self._path_sync_lock_counts)
        has_queued = any(self._is_job_runnable(j) for j in self.jobs)
        selected = self._selected_job()
        can_start_selected = self._is_job_runnable(selected)
        ui_state = build_ui_state_model(
            queue_active=bool(self.queue_active),
            queue_paused=bool(self.queue_paused),
            render_job_active=bool(render_job_active),
            scan_in_progress=bool(scan_in_progress),
            create_job_scan_in_progress=bool(create_job_scan_in_progress),
            hbatch_ok=bool(hbatch_ok),
            path_sync_in_progress=bool(path_sync_in_progress),
            experimental_chunking_enabled=bool(self._experimental_chunking_enabled()),
            chunking_checked=bool(self.chk_enable_chunking.isChecked()),
            has_queued=bool(has_queued),
            can_start_selected=bool(can_start_selected),
            selected_has_log=bool(selected and selected.log_file_path),
        )

        self.add_job_panel.set_enabled_for_run_state(self.queue_active, create_job_scan_in_progress)
        self.btn_preferences.setEnabled(not scan_in_progress)
        if hasattr(self, "btn_reload_all_tree") and self.btn_reload_all_tree is not None:
            self.btn_reload_all_tree.setEnabled(bool(ui_state["reload_all_enabled"]))
        self.chk_enable_chunking.setVisible(bool(ui_state["chunking_visible"]))
        self.spin_chunk_size.setVisible(bool(ui_state["chunking_visible"]))
        if bool(ui_state["force_disable_chunking"]):
            self.chk_enable_chunking.setChecked(False)

        self.btn_start_queue.setEnabled(bool(ui_state["start_enabled"]))
        self.btn_pause_queue.setEnabled(bool(ui_state["pause_enabled"]))
        self.btn_pause_queue.setText(str(ui_state["pause_text"]))
        self.btn_stop_queue.setEnabled(bool(ui_state["stop_enabled"]))
        self.queue_file_menu_button.setEnabled(bool(ui_state["queue_file_menu_enabled"]))
        self.chk_disable_husk_mplay.setEnabled(bool(ui_state["disable_husk_mplay_enabled"]))
        self.chk_enable_chunking.setEnabled(bool(ui_state["chunk_checkbox_enabled"]))
        self.spin_chunk_size.setEnabled(bool(ui_state["chunk_size_enabled"]))
        self.spin_auto_retry.setEnabled(bool(ui_state["auto_retry_enabled"]))
        self.spin_retry_delay.setEnabled(bool(ui_state["retry_delay_enabled"]))
        self.btn_open_log_file.setEnabled(bool(ui_state["selected_has_log"]))
        self._update_job_properties_panel()

        status_message = str(ui_state["status_message"] or "")
        if status_message:
            self._set_status_message(status_message)

    def _reload_all_jobs_from_files(self) -> None:
        if self._scan_in_progress():
            return
        run_reload_all_jobs_from_file_model(
            self.jobs,
            running_status=JobStatus.RUNNING,
            write_queue_snapshot=self._write_queue_snapshot,
            defer_reload_jobs_from_file=self._defer_reload_jobs_from_file,
            set_status_message=self._set_status_message,
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
        if isinstance(self._left_stack_splitter_sizes_pref, list) and len(self._left_stack_splitter_sizes_pref) == 4:
            self.config.set("left_stack_splitter_sizes", [int(max(0, s)) for s in self._left_stack_splitter_sizes_pref])
        if self._left_pane_min_width_floor is not None:
            self.config.set("left_pane_min_width", int(self._left_pane_min_width_floor))
        self.config.set("hbatch_path", self._current_hbatch_path())
        super().closeEvent(event)

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._apply_left_stack_splitter_default_sizes()
        self._schedule_deferred(self._schedule_panel_width_reconcile, UI_DEFERRED_SETTLE_MS)

    def showEvent(self, event: QtGui.QShowEvent) -> None:
        super().showEvent(event)
        if self._startup_layout_pending:
            if not self._startup_layout_finalize_scheduled:
                self._startup_layout_finalize_scheduled = True
                self._schedule_deferred(self._finalize_startup_layout, UI_DEFERRED_NOW_MS)
            return
        self._schedule_now_and_settled(self._schedule_panel_width_reconcile)

    def changeEvent(self, event: QtCore.QEvent) -> None:
        super().changeEvent(event)
        if event.type() == QtCore.QEvent.Type.WindowStateChange:
            self._schedule_now_and_settled(self._schedule_panel_width_reconcile)


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
