from __future__ import annotations

import os
import re
import subprocess
import sys
import traceback
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
    can_remove_jobs,
    is_job_runnable,
    queue_row_status_label,
)
from atomic_io import read_json_file, write_json_atomic
from houdini_service import (
    build_render_preflight_script as build_render_preflight_script_model,
    ensure_husk_hook_files as ensure_husk_hook_files_model,
    load_houdini_script_text as load_houdini_script_text_model,
    project_houdini_scripts_dir as project_houdini_scripts_dir_model,
    required_houdini_script_filenames as required_houdini_script_filenames_model,
    validate_houdini_script_files as validate_houdini_script_files_model,
)
from queue_editing import (
    apply_queue_frame_override_text,
    clear_job_resume_runtime_state as clear_job_resume_runtime_state_model,
    mark_job_offline as mark_job_offline_model,
    reset_job_state as reset_job_state_model,
    restore_job_online_status as restore_job_online_status_model,
)
from queue_file_controller import QueueFileController, QueueFileControllerHooks
from queue_models import FrameHandlingMode, JobStatus, RenderJob
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
    apply_render_finished_state as apply_render_finished_state_model,
    mark_job_done_without_render as mark_job_done_without_render_model,
    plan_frame_handling as plan_frame_handling_model,
    retry_current_chunk as retry_current_chunk_model,
    select_next_runnable_job,
)
from queue_runtime_state import (
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
    validate_queue_path_value as validate_queue_path_value_model,
)
from render_session import RenderSessionController, RenderSessionHooks
from scan_coordinator import ScanCoordinator, ScanCoordinatorHooks
from render_output_parser import (
    detect_phase_from_output_with_job as detect_phase_from_output_with_job_model,
)
from rop_metadata import (
    RopInfo,
    apply_rop_info_to_job as apply_rop_info_to_job_model,
    rop_info_from_scan_record as rop_info_from_scan_record_model,
)
from theme_support import DEFAULT_THEME, build_app_stylesheet, ensure_theme_icons, normalize_theme_colors
from widgets import AddJobPanel, CleanStepSpinBox, PanelFrame, PreferencesDialog, QueueTableItemDelegate, QueueTableWidget
from worker_client import RenderWorkerClient, ScanWorkerClient


APP_NAME = "Houdini Simple Render Manager"
ORG_NAME = "LocalOnly"
CONFIG_DIR_NAME = "HoudiniSimpleRenderManager"
CONFIG_FILE_NAME = "config.json"
THEME_FILE_NAME = "theme.json"
HOUDINI_SCRIPTS_DIR_NAME = "houdini_scripts"


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
            "default_chunking_enabled": False,
            "default_chunk_size": 10,
            "default_retry_count": 1,
            "default_retry_delay": 5,
        }
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = read_json_file(self.path)
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except Exception:
            pass

    def save(self) -> None:
        try:
            write_json_atomic(self.path, self.data)
        except Exception:
            pass

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
        except Exception:
            pass
        return theme

    def save_theme(self, theme: dict[str, str]) -> None:
        try:
            payload = {k: str(v) for k, v in theme.items() if k in DEFAULT_THEME}
            write_json_atomic(self.theme_path, payload)
        except Exception:
            pass


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
        self._active_render_request_id = ""
        self._active_scan_request_id = ""
        self._render_finished_message_received = False
        self._active_hbatch_pid = 0
        self._pending_queue_refresh_args: dict[str, Any] | None = None
        self._pending_queue_refresh_timer = QtCore.QTimer(self)
        self._pending_queue_refresh_timer.setSingleShot(True)
        self._pending_queue_refresh_timer.timeout.connect(self._flush_pending_queue_refresh)
        self._houdini_scripts_missing_warned = False
        self._suppress_queue_item_changed = False
        self._queue_header_group_restore_guard = False
        self._queue_header_valid_order: list[int] = []
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
        self.scan_worker_client = ScanWorkerClient(
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
        self.render_worker_client.message_received.connect(self._handle_render_worker_message)
        self.render_worker_client.stderr_received.connect(self._append_render_worker_stderr)
        self.render_worker_client.worker_failed.connect(self._on_render_worker_failed)
        self.scan_coordinator = ScanCoordinator(
            ScanCoordinatorHooks(
                current_hbatch_path=self._current_hbatch_path,
                project_houdini_scripts_dir=self._project_houdini_scripts_dir,
                hooks_dir_path=lambda: self.config.hooks_dir,
                hbatch_exists=self._hbatch_exists,
                scan_in_progress=self._scan_in_progress,
                send_scan_request=self._send_scan_worker_request,
                request_scan_sync=lambda message_type, payload, timeout_ms: self._request_scan_worker_sync(
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
                normalize_output_display_path=self._normalize_output_display_path,
                hscript_quote=hscript_quote,
                current_time=datetime.now,
                update_job_render_timing_stats=self._update_job_render_timing_stats,
                update_phase_from_frame_sequence=self._update_phase_from_frame_sequence,
                update_job_phase_from_output=self._update_job_phase_from_output,
                cancel_phase_promote=self._cancel_phase_promote,
                mark_job_offline=self._mark_job_offline,
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
        except Exception:
            self._main_splitter_left_width_pref = None
        saved_top_height = self.config.get("left_splitter_top_height")
        try:
            self._left_splitter_top_height_pref = int(saved_top_height) if saved_top_height is not None else None
        except Exception:
            self._left_splitter_top_height_pref = None
        saved_notifications_height = self.config.get("left_notifications_height")
        try:
            self._left_notifications_height_pref = int(saved_notifications_height) if saved_notifications_height is not None else None
        except Exception:
            self._left_notifications_height_pref = None
        QtCore.QTimer.singleShot(0, self._apply_main_splitter_left_width_pref)
        QtCore.QTimer.singleShot(0, self._apply_left_splitter_default_sizes)

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
        safe_message(self, "Scan Worker", reason, self.scan_worker_client.last_stderr_text or None)
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
        return bool(getattr(self, "chk_enable_chunking", None) and self.chk_enable_chunking.isChecked())

    def _chunk_size_value(self) -> int:
        try:
            return int(getattr(self, "spin_chunk_size", None).value())  # type: ignore[union-attr]
        except Exception:
            return 1

    def _retry_count_value(self) -> int:
        try:
            return int(getattr(self, "spin_auto_retry", None).value())  # type: ignore[union-attr]
        except Exception:
            return 0

    def _retry_delay_value(self) -> int:
        try:
            return int(getattr(self, "spin_retry_delay", None).value())  # type: ignore[union-attr]
        except Exception:
            return 0

    def _default_chunking_enabled(self) -> bool:
        return bool(self.config.get("default_chunking_enabled", False))

    def _default_chunk_size(self) -> int:
        try:
            return max(1, int(self.config.get("default_chunk_size", 10)))
        except Exception:
            return 10

    def _default_retry_count(self) -> int:
        try:
            return max(0, int(self.config.get("default_retry_count", 1)))
        except Exception:
            return 1

    def _default_retry_delay(self) -> int:
        try:
            return max(0, int(self.config.get("default_retry_delay", 5)))
        except Exception:
            return 5

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
        phase = (job.view.phase_text or "").strip()
        if job.runtime.chunk_total_runtime > 1:
            chunk_part = f"Chunk {max(1, job.runtime.chunk_index_runtime + 1)}/{job.runtime.chunk_total_runtime}"
            if job.runtime.chunk_attempt_runtime > 1:
                chunk_part += f" r{job.runtime.chunk_attempt_runtime}"
            if phase:
                return f"{phase} ({chunk_part})"
            return chunk_part
        return phase

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

        controls_row = QtWidgets.QHBoxLayout()
        controls_row.setContentsMargins(8, 8, 8, 8)
        controls_row.setSpacing(8)
        controls_row.addWidget(QtWidgets.QLabel("Recent events"))
        controls_row.addStretch(1)
        self.btn_clear_notifications = QtWidgets.QPushButton("Clear")
        self.btn_clear_notifications.clicked.connect(self._clear_notifications_view_only)
        controls_row.addWidget(self.btn_clear_notifications)
        layout.addLayout(controls_row)

        self.notifications_list = QtWidgets.QListWidget()
        self.notifications_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.notifications_list.setAlternatingRowColors(True)
        self.notifications_list.setUniformItemSizes(False)
        self.notifications_list.setWordWrap(True)
        self.notifications_list.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.notifications_list.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.notifications_list.verticalScrollBar().setSingleStep(20)
        layout.addWidget(self.notifications_list, 1)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        self.notifications_frame = PanelFrame("Notifications", box)
        self.notifications_frame.set_body_margins(0, 0, 0, 0)
        return self.notifications_frame

    def _build_tree_view_panel(self) -> QtWidgets.QWidget:
        panel, self.queue_tree, self.queue_tree_model = build_queue_tree_panel_model(
            self,
            item_changed_handler=self._on_queue_tree_item_changed,
        )
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

        sep_chunk_retry = QtWidgets.QFrame()
        sep_chunk_retry.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep_chunk_retry.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        sep_chunk_retry.setObjectName("toolbarSeparator")
        sep_buttons_chunk = QtWidgets.QFrame()
        sep_buttons_chunk.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep_buttons_chunk.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        sep_buttons_chunk.setObjectName("toolbarSeparator")
        sep_retry_monitor = QtWidgets.QFrame()
        sep_retry_monitor.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep_retry_monitor.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
        sep_retry_monitor.setObjectName("toolbarSeparator")

        controls_row.addWidget(self.btn_start_queue)
        controls_row.addWidget(self.btn_pause_queue)
        controls_row.addWidget(self.btn_stop_queue)
        controls_row.addSpacing(4)
        controls_row.addWidget(sep_buttons_chunk)
        controls_row.addSpacing(4)
        controls_row.addWidget(self.chk_enable_chunking)
        controls_row.addWidget(self.spin_chunk_size)
        controls_row.addSpacing(4)
        controls_row.addWidget(sep_chunk_retry)
        controls_row.addSpacing(4)
        controls_row.addWidget(self.lbl_retry)
        controls_row.addWidget(self.spin_auto_retry)
        controls_row.addWidget(self.lbl_delay)
        controls_row.addWidget(self.spin_retry_delay)
        controls_row.addSpacing(4)
        controls_row.addWidget(sep_retry_monitor)
        controls_row.addSpacing(4)
        controls_row.addWidget(self.chk_disable_husk_mplay)
        controls_row.addStretch(1)
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
        layout.addWidget(controls_host)

        self.queue_table = QueueTableWidget(0, 17)
        self.queue_table.setHorizontalHeaderLabels(
            [
                "Name",
                "HIP",
                "ROP",
                "Frame Range",
                "Step",
                "Frame Handling",
                "Status",
                "Progress",
                "Phase",
                "Remaining",
                "Frame",
                "Frame Time",
                "Avg Frame Time",
                "Started",
                "Completed",
                "Render Time",
                "Output",
            ]
        )
        self.queue_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.queue_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.queue_table.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.DoubleClicked
            | QtWidgets.QAbstractItemView.EditTrigger.EditKeyPressed
        )
        self.queue_table.setAlternatingRowColors(True)
        self.queue_table.setItemDelegate(QueueTableItemDelegate(self.queue_table))
        self.queue_table.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.queue_table.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollMode.ScrollPerPixel)
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
        self.queue_table.setColumnWidth(0, 220)
        self.queue_table.setColumnWidth(1, 260)
        self.queue_table.setColumnWidth(2, 220)
        self.queue_table.setColumnWidth(3, 95)
        self.queue_table.setColumnWidth(4, 70)
        self.queue_table.setColumnWidth(5, 170)
        self.queue_table.setColumnWidth(6, 80)
        self.queue_table.setColumnWidth(7, 110)
        self.queue_table.setColumnWidth(8, 90)
        self.queue_table.setColumnWidth(9, 100)
        self.queue_table.setColumnWidth(10, 95)
        self.queue_table.setColumnWidth(11, 85)
        self.queue_table.setColumnWidth(12, 90)
        self.queue_table.setColumnWidth(13, 90)
        self.queue_table.setColumnWidth(14, 100)
        self.queue_table.setColumnWidth(15, 100)
        self.queue_table.setColumnWidth(16, 260)
        self._queue_default_column_widths = {
            logical: int(self.queue_table.columnWidth(logical)) for logical in range(self.queue_table.columnCount())
        }
        self.queue_table.stats_split_after_visual_index = 6
        self.queue_table.itemSelectionChanged.connect(self._on_queue_selection_changed)
        self.queue_table.itemChanged.connect(self._on_queue_item_changed)
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
        layout.addWidget(self.queue_table)

        box.setObjectName("panelEmbeddedGroup")
        box.setTitle("")
        panel = PanelFrame("Render Queue", box)
        panel.set_body_margins(0, 0, 0, 0)
        return panel

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
        self._hbatch_path = hbatch_path
        self._save_hbatch_path()
        self.config.set("player_path", player_path)
        if isinstance(theme, dict):
            self.theme = normalize_theme_colors(theme)
            self.config.save_theme(self.theme)
            self._apply_theme()
        if isinstance(runtime_defaults, dict):
            try:
                chunking_enabled = bool(runtime_defaults.get("chunking_enabled", False))
                chunk_size = max(1, int(runtime_defaults.get("chunk_size", 10)))
                retry_count = max(0, int(runtime_defaults.get("retry_count", 1)))
                retry_delay = max(0, int(runtime_defaults.get("retry_delay", 5)))
            except Exception:
                chunking_enabled = False
                chunk_size = 10
                retry_count = 1
                retry_delay = 5
            self.config.set("default_chunking_enabled", chunking_enabled)
            self.config.set("default_chunk_size", chunk_size)
            self.config.set("default_retry_count", retry_count)
            self.config.set("default_retry_delay", retry_delay)
            if hasattr(self, "chk_enable_chunking"):
                self.chk_enable_chunking.setChecked(chunking_enabled)
            if hasattr(self, "spin_chunk_size"):
                self.spin_chunk_size.setValue(chunk_size)
            if hasattr(self, "spin_auto_retry"):
                self.spin_auto_retry.setValue(retry_count)
            if hasattr(self, "spin_retry_delay"):
                self.spin_retry_delay.setValue(retry_delay)
            self._refresh_ui_state()

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
            interrupted_count = sum(1 for job in self.jobs if job.runtime.status == JobStatus.INTERRUPTED)
            if self.jobs:
                self._refresh_queue_table(select_row=0)
            else:
                self._refresh_queue_table()
            if interrupted_count:
                self._append_log("Stderr", f"[Recovery] Recovered {interrupted_count} interrupted job(s) from the last session.\n")
                self._set_status_message(f"Recovered {interrupted_count} interrupted job(s)", 6000)
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
                output_hint = self.add_job_panel.rop_output_path_for_path(job.spec.rop_path)
                if output_hint:
                    job.view.out_file_sample_path = output_hint
                    job.view.out_path = self._normalize_output_display_path(output_hint)
                rs, re_, rstep = self.add_job_panel.rop_range_info_for_path(job.spec.rop_path)
                if rs is not None and re_ is not None:
                    job.runtime.runtime_start_frame = rs
                    job.runtime.runtime_end_frame = re_
                    job.runtime.runtime_step = rstep

            if job.spec.frame_range_mode == "use_rop":
                if job.runtime.runtime_start_frame is None or job.runtime.runtime_end_frame is None:
                    try:
                        probe_err = self._probe_and_apply_job_rop_metadata(job)
                    except Exception as exc:
                        probe_err = f"probe_failed: {exc}"
                    if probe_err == "node_not_found":
                        self._mark_job_offline(job, "ROP node not found in HIP file.")
                    elif probe_err:
                        self._append_log("Stderr", f"[Add Job] Could not resolve ROP range for {job.spec.rop_path}: {probe_err}\n")
                        if str(probe_err).startswith("probe_failed:"):
                            self._mark_job_offline(job, str(probe_err))

            if job.spec.frame_range_mode == "override":
                strict_probe = self._probe_rop_strict_frame_range(job.spec.hip_path, job.spec.rop_path)
                if strict_probe is not None and hasattr(self, "add_job_panel"):
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
        )
        job.runtime.log_file_path = str(self.config.new_job_log_path(job.display_name()))
        job.view.phase_text = ""
        job.view.progress_text = "-"
        job.view.percent_text = "-"
        return job

    def _probe_rop_info(self, hip_path: str, rop_path: str) -> RopInfo | None:
        return self.scan_coordinator.probe_rop_info(hip_path, rop_path)

    def _probe_and_apply_job_rop_metadata(self, job: RenderJob) -> str | None:
        return self.scan_coordinator.probe_and_apply_job_rop_metadata(job)

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

    def _selected_row(self) -> int:
        rows = self.queue_table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _selected_rows(self) -> list[int]:
        model = self.queue_table.selectionModel()
        if model is None:
            return []
        rows = sorted({idx.row() for idx in model.selectedRows() if idx.isValid()})
        return [r for r in rows if 0 <= r < len(self.jobs)]

    def _selected_jobs(self) -> list[RenderJob]:
        return [self.jobs[r] for r in self._selected_rows() if 0 <= r < len(self.jobs)]

    def _selected_job_ids(self) -> list[str]:
        return [job.id for job in self._selected_jobs()]

    def _refresh_queue_preserve_selection(self) -> None:
        selected_ids = self._selected_job_ids()
        selected = self._selected_job()
        self._refresh_queue_table(
            select_job_ids=selected_ids or None,
            select_job_id=None if selected_ids else (selected.id if selected is not None else None),
        )

    def _queue_refresh_should_defer(self) -> bool:
        focus = QtWidgets.QApplication.focusWidget()
        if focus is None:
            return False
        if self.queue_table.state() == QtWidgets.QAbstractItemView.State.EditingState:
            return True
        if focus is self.queue_table or self.queue_table.isAncestorOf(focus):
            return isinstance(
                focus,
                (
                    QtWidgets.QLineEdit,
                    QtWidgets.QAbstractSpinBox,
                    QtWidgets.QComboBox,
                    QtWidgets.QPlainTextEdit,
                    QtWidgets.QTextEdit,
                ),
            )
        if hasattr(self, "add_job_panel") and self.add_job_panel is not None and self.add_job_panel.isAncestorOf(focus):
            return isinstance(
                focus,
                (
                    QtWidgets.QLineEdit,
                    QtWidgets.QAbstractSpinBox,
                    QtWidgets.QComboBox,
                    QtWidgets.QPlainTextEdit,
                    QtWidgets.QTextEdit,
                    QtWidgets.QListWidget,
                ),
            )
        return False

    def _flush_pending_queue_refresh(self) -> None:
        if not self._pending_queue_refresh_args:
            return
        if self._queue_refresh_should_defer():
            self._pending_queue_refresh_timer.start(200)
            return
        args = dict(self._pending_queue_refresh_args)
        self._pending_queue_refresh_args = None
        self._refresh_queue_table(**args)

    @staticmethod
    def _is_job_runnable(job: RenderJob | None) -> bool:
        return is_job_runnable(job)

    def _reset_job_state(self, job: RenderJob) -> None:
        reset_job_state_model(job)

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

    def _defer_save_and_refresh_queue(self, select_job_ids: list[str] | None = None) -> None:
        ids = list(select_job_ids or [])
        QtCore.QTimer.singleShot(
            0,
            lambda ids=ids: self._save_and_refresh_queue(
                select_job_ids=self._selection_ids_for_refresh(ids)
            ),
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
        act_remove.setEnabled(any(not self._is_active_job(job) for job in target_jobs))

        chosen = menu.exec(self.queue_tree.viewport().mapToGlobal(pos))
        if chosen is None:
            return
        if chosen == act_select:
            self._refresh_queue_table(select_job_ids=[job.id for job in target_jobs])
            return
        if chosen == act_remove:
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
                target_ids = [job.id for job in self.jobs if job.spec.hip_path == old_hip]
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
                changed_ids = self._propagate_hip_path_change(old_hip, text)
                after_states = self._job_states_for_ids(changed_ids)
                if changed_ids:
                    self._push_history_command(
                        {
                            "kind": "update_jobs",
                            "before": before_states,
                            "after": after_states,
                            "undo_select_job_ids": target_ids,
                            "redo_select_job_ids": changed_ids,
                        }
                    )
                self._defer_save_and_refresh_queue(changed_ids)
                return
            if kind == "rop":
                target_ids = [job.id for job in self.jobs if job.spec.hip_path == old_hip and job.spec.rop_path == old_rop]
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
                changed_ids = self._propagate_rop_path_change(old_hip, old_rop, text)
                after_states = self._job_states_for_ids(changed_ids)
                if changed_ids:
                    self._push_history_command(
                        {
                            "kind": "update_jobs",
                            "before": before_states,
                            "after": after_states,
                            "undo_select_job_ids": target_ids,
                            "redo_select_job_ids": changed_ids,
                        }
                    )
                self._defer_save_and_refresh_queue(changed_ids)
                return
        except Exception as exc:
            safe_message(self, "Tree Edit Error", f"Failed to apply tree edit: {exc}", traceback.format_exc())
            self._defer_refresh_queue_tree_view()

    @staticmethod
    def _job_can_reset_cached_cell(job: RenderJob, col: int) -> bool:
        if job.runtime.status == JobStatus.RUNNING:
            return False
        if col == 3:
            return (
                job.runtime.runtime_start_frame is not None
                and job.runtime.runtime_end_frame is not None
                and not job.spec.strict_frame_range
            )
        if col == 4:
            return (job.runtime.runtime_step not in (None, 0)) and (not job.spec.strict_frame_range)
        return False

    @staticmethod
    def _normalize_job_override_mode_against_cached(job: RenderJob) -> None:
        if job.spec.frame_range_mode != "override":
            return
        if (
            job.runtime.runtime_start_frame is None
            or job.runtime.runtime_end_frame is None
            or job.runtime.runtime_step in (None, 0)
            or job.spec.start_frame is None
            or job.spec.end_frame is None
            or job.spec.step is None
        ):
            return
        try:
            matches_range = (
                int(job.spec.start_frame) == int(float(job.runtime.runtime_start_frame))
                and int(job.spec.end_frame) == int(float(job.runtime.runtime_end_frame))
            )
            matches_step = int(job.spec.step) == int(float(job.runtime.runtime_step))
        except Exception:
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
            if col == 3:
                try:
                    rs = int(float(target.runtime_start_frame))
                    re = int(float(target.runtime_end_frame))
                except Exception:
                    continue
                if target.frame_range_mode == "use_rop":
                    continue
                target.start_frame = rs
                target.end_frame = re
                changed = True
            elif col == 4:
                try:
                    rstep = int(float(target.runtime_step))
                except Exception:
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
        act_toggle = menu.addAction("Disable" if job.spec.enabled else "Enable")
        act_toggle.setEnabled(not any_active)
        act_reset = menu.addAction("Reset State")
        act_reset.setEnabled(not any_active)
        act_reset_cell_cached = None
        if idx.isValid() and idx.column() in {3, 4}:
            menu.addSeparator()
            act_reset_cell_cached = menu.addAction("Reset Value")
            act_reset_cell_cached.setEnabled(any(self._job_can_reset_cached_cell(t, idx.column()) for t in target_jobs))
        act_reload_from_rop = menu.addAction("Reload from File")
        act_reload_from_rop.setEnabled(bool((not any_active) and self._hbatch_exists()))
        menu.addSeparator()
        duplicate_decision = can_duplicate_jobs(
            target_jobs,
            is_active_job_fn=self._is_active_job,
            scan_in_progress=self._scan_in_progress(),
        )
        act_duplicate = menu.addAction("Duplicate")
        act_duplicate.setEnabled(duplicate_decision.allowed)
        act_remove = menu.addAction("Remove")
        act_remove.setEnabled(not any_active)
        act_clear_finished = menu.addAction("Clear Finished")
        act_clear_finished.setEnabled(has_finished_jobs)
        menu.addSeparator()
        act_preview = menu.addAction("Preview")
        act_preview.setEnabled(bool(self._current_player_path()) and bool(self._job_preview_path(job)))
        act_open_folder = menu.addAction("Open Folder")
        act_open_folder.setEnabled(bool(out_folder and out_folder.exists()))

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
            target_ids = [j.id for j in target_jobs]
            before_states = self._job_states_for_ids(target_ids)
            try:
                changed_ids = self._refresh_jobs_from_rop_metadata(target_jobs, reset_override_to_rop=True)
            except Exception as exc:
                safe_message(self, "Reload Failed", f"Failed to reload job metadata: {exc}", traceback.format_exc())
                self._refresh_queue_preserve_selection()
                return
            if changed_ids:
                after_states = self._job_states_for_ids(changed_ids)
                self._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_ids,
                        "redo_select_job_ids": [j.id for j in target_jobs],
                    }
                )
                self._save_and_refresh_queue(select_job_ids=[j.id for j in target_jobs])
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
        text = str(sample_path or "").strip()
        p = Path(text)
        if not p.name or text.lower() == "ip":
            return None
        # Handle common Houdini frame tokens directly (scan metadata often contains tokenized paths).
        frame_abs = abs(int(frame))
        frame_sign = "-" if int(frame) < 0 else ""

        def _pad(width: int | None) -> str:
            w = max(1, int(width or 1))
            return f"{frame_sign}{frame_abs:0{w}d}"

        name = p.name
        token_patterns: list[tuple[str, re.Pattern[str]]] = [
            ("angle", re.compile(r"<F(\d*)>", re.IGNORECASE)),
            ("brace", re.compile(r"\$\{F(\d*)\}", re.IGNORECASE)),
            ("dollar", re.compile(r"\$F(\d*)", re.IGNORECASE)),
        ]
        for _label, pat in token_patterns:
            if pat.search(name):
                replaced = pat.sub(lambda m: _pad(int(m.group(1)) if m.group(1) else None), name)
                return p.with_name(replaced)

        # If the path still appears tokenized (other formats), do not guess from incidental digits.
        if any(tok in name for tok in ("$F", "${F", "<F", "%0")):
            return None

        stem = p.stem
        m = re.search(r"(-?\d+)(?!.*\d)", stem)
        if not m:
            return None
        token = m.group(1)
        negative = token.startswith("-")
        width = len(token) - (1 if negative else 0)
        prefix = stem[: m.start(1)]
        suffix = stem[m.end(1) :]
        if frame < 0:
            body = f"-{abs(frame):0{max(1, width)}d}"
        else:
            body = f"{frame:0{max(1, width)}d}"
        filename = f"{prefix}{body}{suffix}{p.suffix}"
        return p.with_name(filename)

    @staticmethod
    def _normalize_output_display_path(path_text: str) -> str:
        text = str(path_text or "").strip()
        if not text:
            return ""
        if text.lower() == "ip":
            return "ip"
        p = Path(text)
        return str(p.parent if p.suffix else p)

    @staticmethod
    def _output_folder_from_value(path_text: str) -> Path | None:
        text = str(path_text or "").strip()
        if not text or text.lower() == "ip":
            return None
        p = Path(text)
        return p.parent if p.suffix else p

    def _compute_resume_from_output(
        self,
        job: RenderJob,
        *,
        interactive: bool = True,
    ) -> tuple[int, int, int, int] | None:
        sample_file_path = (job.view.out_file_sample_path or "").strip()
        out_path = (job.view.out_path or "").strip()
        probe_path = sample_file_path or out_path
        if job.spec.strict_frame_range:
            if interactive:
                safe_message(self, "Resume From Output", "Cannot resume from output on a Strict frame range ROP.")
            return None
        resolved = self._resolve_job_range_for_execution(job, mutate_job=False)
        if resolved is None:
            if job.runtime.status == JobStatus.OFFLINE:
                return None
            if interactive:
                safe_message(self, "Resume From Output", "Cannot resolve frame range for this job.")
            return None
        start_frame, end_frame, step = resolved
        if step <= 0 or end_frame < start_frame:
            if interactive:
                safe_message(self, "Resume From Output", "Invalid job frame range for resume.")
            return None

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

        if not probe_path or probe_path.lower() == "ip":
            if interactive:
                safe_message(self, "Resume From Output", "Cannot resume: output path is unavailable.")
            return None

        if self._frame_sequence_path_for_frame(probe_path, start_frame) is None:
            if interactive:
                safe_message(
                    self,
                    "Resume From Output",
                    "Could not resolve a reliable output filename pattern from the ROP. "
                    "Use Reload from ROP (or start a render once) so the app can capture the exact output pattern.",
                )
            return None

        total = ((end_frame - start_frame) // step) + 1
        contiguous_done = 0
        first_missing: int | None = None
        for frame in range(start_frame, end_frame + 1, step):
            expected = self._frame_sequence_path_for_frame(probe_path, frame)
            if expected is None:
                return None
            try:
                exists = expected.exists()
                size_ok = expected.stat().st_size > 0 if exists else False
            except Exception:
                exists = False
                size_ok = False
            if exists and size_ok and first_missing is None:
                contiguous_done += 1
                continue
            first_missing = frame
            break

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
        if resolved is None:
            if interactive and job.runtime.status != JobStatus.OFFLINE:
                safe_message(self, "Render Missing", "Cannot resolve frame range for this job.")
            return None
        start_frame, end_frame, step = resolved
        if step <= 0 or end_frame < start_frame:
            if interactive:
                safe_message(self, "Render Missing", "Invalid job frame range.")
            return None

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

        if not probe_path or probe_path.lower() == "ip":
            if interactive:
                safe_message(self, "Render Missing", "Cannot evaluate outputs: output path is unavailable.")
            return None

        if self._frame_sequence_path_for_frame(probe_path, start_frame) is None:
            if interactive:
                safe_message(
                    self,
                    "Render Missing",
                    "Could not resolve a reliable output filename pattern from the ROP.",
                )
            return None

        existing_count = 0
        missing_frames: list[int] = []
        for frame in range(start_frame, end_frame + 1, step):
            expected = self._frame_sequence_path_for_frame(probe_path, frame)
            if expected is None:
                return None
            try:
                exists = expected.exists()
                size_ok = expected.stat().st_size > 0 if exists else False
            except Exception:
                exists = False
                size_ok = False
            if exists and size_ok:
                existing_count += 1
            else:
                missing_frames.append(frame)

        if not missing_frames:
            return [], existing_count

        runs: list[tuple[int, int, int]] = []
        run_start = missing_frames[0]
        run_prev = missing_frames[0]
        for frame in missing_frames[1:]:
            if frame == run_prev + step:
                run_prev = frame
                continue
            runs.append((run_start, run_prev, step))
            run_start = frame
            run_prev = frame
        runs.append((run_start, run_prev, step))
        return runs, existing_count

    def _resume_job_from_output(self, job: RenderJob) -> None:
        if self._render_job_active() or self.queue_active:
            return
        if job.runtime.status not in {JobStatus.CANCELED, JobStatus.INTERRUPTED, JobStatus.QUEUED, JobStatus.DONE}:
            return
        if not Path(job.spec.hip_path).exists():
            self._mark_job_offline(job, "HIP file not found.")
            self._save_queue_state()
            self._refresh_queue_table(select_job_id=job.id)
            return
        if not self._hbatch_exists():
            safe_message(self, "hbatch Missing", "Configure a valid hbatch.exe path before resuming.")
            return
        if not job.spec.enabled:
            safe_message(self, "Resume From Output", "Job is disabled.")
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
        if self._history_applying:
            return
        kind = str(command.get("kind", "") or "")
        if kind in {"insert_jobs", "remove_jobs"} and not list(command.get("entries", []) or []):
            return
        if kind == "update_jobs" and list(command.get("before", []) or []) == list(command.get("after", []) or []):
            return
        if kind == "reorder_jobs" and list(command.get("before_order", []) or []) == list(command.get("after_order", []) or []):
            return
        self._undo_stack.append(command)
        if len(self._undo_stack) > 100:
            self._undo_stack = self._undo_stack[-100:]
        self._redo_stack.clear()

    def _clear_history(self) -> None:
        self._undo_stack.clear()
        self._redo_stack.clear()

    def _history_command_targets_active_job(self, command: dict[str, Any]) -> bool:
        active_job_id = str(self.current_job_id or "")
        if not active_job_id:
            return False
        candidate_ids: set[str] = set()
        for key in ("before", "after"):
            for state in list(command.get(key, []) or []):
                if isinstance(state, dict):
                    job_id = str(state.get("id", "") or "").strip()
                    if job_id:
                        candidate_ids.add(job_id)
        for entry in list(command.get("entries", []) or []):
            if not isinstance(entry, dict):
                continue
            job_state = entry.get("job")
            if isinstance(job_state, dict):
                job_id = str(job_state.get("id", "") or "").strip()
                if job_id:
                    candidate_ids.add(job_id)
        for key in ("before_order", "after_order", "undo_select_job_ids", "redo_select_job_ids"):
            for job_id in list(command.get(key, []) or []):
                text = str(job_id or "").strip()
                if text:
                    candidate_ids.add(text)
        return active_job_id in candidate_ids

    def _apply_history_command(self, command: dict[str, Any], *, undo: bool) -> None:
        kind = str(command.get("kind", "") or "")
        undo_select = list(command.get("undo_select_job_ids", []) or [])
        redo_select = list(command.get("redo_select_job_ids", []) or [])
        select_ids = undo_select if undo else redo_select
        self._history_applying = True
        try:
            if kind == "insert_jobs":
                entries = list(command.get("entries", []) or [])
                if undo:
                    self._remove_jobs_by_ids([str(entry.get("job", {}).get("id", "") or "") for entry in entries])
                else:
                    self._insert_jobs_from_entries(entries)
            elif kind == "remove_jobs":
                entries = list(command.get("entries", []) or [])
                if undo:
                    self._insert_jobs_from_entries(entries)
                else:
                    self._remove_jobs_by_ids([str(entry.get("job", {}).get("id", "") or "") for entry in entries])
            elif kind == "update_jobs":
                states = list(command.get("before", []) if undo else command.get("after", []))
                self._apply_job_states(states)
            elif kind == "reorder_jobs":
                order = list(command.get("before_order", []) if undo else command.get("after_order", []))
                self._apply_job_order(order)
        finally:
            self._history_applying = False
        self._save_queue_state()
        self._refresh_queue_table(select_job_ids=select_ids or None)

    def _undo_queue_edit(self) -> None:
        if self._scan_in_progress():
            return
        if not self._undo_stack:
            return
        if self._history_command_targets_active_job(self._undo_stack[-1]):
            return
        command = self._undo_stack.pop()
        self._apply_history_command(command, undo=True)
        self._redo_stack.append(command)
        self._set_status_message("Undo", 1500)

    def _redo_queue_edit(self) -> None:
        if self._scan_in_progress():
            return
        if not self._redo_stack:
            return
        if self._history_command_targets_active_job(self._redo_stack[-1]):
            return
        command = self._redo_stack.pop()
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
        except Exception as exc:
            self._append_log("Stderr", f"[Queue] Failed to save queue: {exc}\n")
            return False

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
        remove_decision = can_remove_jobs(selected_jobs, is_active_job_fn=self._is_active_job)
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
            clone = self._job_from_persisted_dict(self._job_to_persisted_dict(source))
            if clone is None:
                continue
            clone.id = uuid4().hex
            clone.status = JobStatus.QUEUED
            clone.started_at = None
            clone.finished_at = None
            clone.exit_code = None
            clone.error_summary = ""
            clone.offline_detected_reason = ""
            clone.progress_text = "-"
            clone.percent_text = "-"
            clone.usd_build_percent = None
            clone.last_frame_seen = None
            clone.build_pass_completed = False
            clone.phase_text = ""
            clone.prev_frame_time_text = "-"
            clone.avg_frame_time_text = "-"
            clone.est_job_time_text = "-"
            clone.render_frame_started_at = {}
            clone.render_frame_durations_sec = []
            clone.render_completed_frames = set()
            clone.offline_previous_status = None
            clone.resume_start_frame_runtime = None
            clone.resume_end_frame_runtime = None
            clone.resume_step_runtime = None
            clone.resume_completed_baseline_count = 0
            clone.chunk_start_frame_runtime = None
            clone.chunk_end_frame_runtime = None
            clone.chunk_step_runtime = None
            clone.chunk_index_runtime = 0
            clone.chunk_total_runtime = 0
            clone.chunk_attempt_runtime = 0
            clone.chunk_retry_count_runtime = 0
            clone.chunk_ranges_runtime = []
            clone.chunk_retry_total_failures_runtime = 0
            clone.log_file_path = str(self.config.new_job_log_path(clone.display_name()))
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
        rows = sorted({int(r) for r in source_rows if 0 <= int(r) < len(self.jobs)})
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
            self._pending_queue_refresh_args = {
                "select_row": select_row,
                "select_job_id": select_job_id,
                "select_job_ids": list(select_job_ids or []) or None,
            }
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

        table_blocker = QtCore.QSignalBlocker(self.queue_table)
        selection_model = self.queue_table.selectionModel()
        selection_blocker = QtCore.QSignalBlocker(selection_model) if selection_model is not None else None
        try:
            self._suppress_queue_item_changed = True
            self.queue_table.setUpdatesEnabled(False)
            self.queue_table.setRowCount(len(self.jobs))
            self.queue_table.clearSelection()
            for row, job in enumerate(self.jobs):
                self._populate_queue_row(row, job)

            target_job_id = select_job_id or preserved_job_id
            target_job_ids = list(select_job_ids or preserved_job_ids)
            selected_applied = False
            if target_job_ids:
                sm = self.queue_table.selectionModel()
                if sm is not None:
                    for row, job in enumerate(self.jobs):
                        if job.id in target_job_ids:
                            model_idx = self.queue_table.model().index(row, 0)
                            sm.select(
                                model_idx,
                                QtCore.QItemSelectionModel.SelectionFlag.Select
                                | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                            )
                            selected_applied = True
            elif target_job_id:
                for row, job in enumerate(self.jobs):
                    if job.id == target_job_id:
                        self.queue_table.selectRow(row)
                        selected_applied = True
                        break
            elif select_row is not None and self.jobs:
                self.queue_table.selectRow(max(0, min(select_row, len(self.jobs) - 1)))
                selected_applied = True

            if selected_applied:
                sm = self.queue_table.selectionModel()
                row = self._selected_row()
                if sm is not None and row >= 0:
                    idx = self.queue_table.model().index(row, 0)
                    sm.setCurrentIndex(idx, QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)
        finally:
            self._suppress_queue_item_changed = False
            self.queue_table.setUpdatesEnabled(True)
            del table_blocker
            if selection_blocker is not None:
                del selection_blocker

        self._refresh_queue_tree_view()
        self._refresh_ui_state()

    def _populate_queue_row(self, row: int, job: RenderJob) -> None:
        values = [
            job.display_name(),
            job.spec.hip_path,
            job.spec.rop_path,
            job.frame_range_display(),
            job.step_display(),
            job.frame_handling_label(),
            queue_row_status_label(job),
            job.view.percent_text or "",
            self._job_phase_display(job),
            self._job_time_remaining_display(job),
            self._job_frame_display(job),
            job.view.prev_frame_time_text or "-",
            job.view.avg_frame_time_text or "-",
            self._job_started_time_display(job),
            self._job_end_time_display(job),
            self._job_total_time_display(job),
            job.view.out_path or "",
        ]
        for col, value in enumerate(values):
            item = self.queue_table.item(row, col)
            if item is None:
                item = QtWidgets.QTableWidgetItem()
                self.queue_table.setItem(row, col, item)
            item.setText(value)
            item.setData(QtCore.Qt.ItemDataRole.UserRole + 20, str(job.runtime.status.value))
            if col == 7:
                build_pct, render_pct = self._queue_progress_split_values(job)
                item.setData(QtCore.Qt.ItemDataRole.UserRole + 10, build_pct)
                item.setData(QtCore.Qt.ItemDataRole.UserRole + 11, render_pct)
            flags = item.flags()
            edit_decision = can_edit_job_column(
                job,
                column=col,
                is_active_job=self._is_active_job(job),
            )
            if edit_decision.allowed:
                item.setFlags(flags | QtCore.Qt.ItemFlag.ItemIsEditable)
            else:
                item.setFlags(flags & ~QtCore.Qt.ItemFlag.ItemIsEditable)
            if col == 5:
                item.setIcon(QtGui.QIcon())
                item.setToolTip("How this job treats existing output frames before render.")
            if col in {3, 4}:
                range_is_overridden = False
                step_is_overridden = False
                if job.spec.frame_range_mode == "override":
                    if (
                        job.runtime.runtime_start_frame is None
                        or job.runtime.runtime_end_frame is None
                        or job.spec.start_frame is None
                        or job.spec.end_frame is None
                    ):
                        range_is_overridden = True
                    else:
                        try:
                            range_is_overridden = not (
                                int(job.spec.start_frame) == int(job.runtime.runtime_start_frame)
                                and int(job.spec.end_frame) == int(job.runtime.runtime_end_frame)
                            )
                        except Exception:
                            range_is_overridden = True

                    if job.runtime.runtime_step in (None, 0) or job.spec.step is None:
                        step_is_overridden = True
                    else:
                        try:
                            step_is_overridden = int(job.spec.step) != int(float(job.runtime.runtime_step))
                        except Exception:
                            step_is_overridden = True

                if job.spec.strict_frame_range:
                    lock_path = str(getattr(self, "_theme_icons", {}).get("lock_orange", "") or "")
                    item.setIcon(QtGui.QIcon(lock_path) if lock_path else QtGui.QIcon())
                    item.setToolTip("ROP frame range is Strict (node-controlled).")
                elif (col == 3 and range_is_overridden) or (col == 4 and step_is_overridden):
                    dot_path = str(getattr(self, "_theme_icons", {}).get("override_dot_red", "") or "")
                    item.setIcon(QtGui.QIcon(dot_path) if dot_path else QtGui.QIcon())
                    item.setToolTip("Overridden value.")
                else:
                    item.setIcon(QtGui.QIcon())
                    if job.spec.frame_range_mode == "use_rop":
                        item.setToolTip("Using ROP value.")
                    elif col == 3:
                        item.setToolTip("Range matches ROP value.")
                    else:
                        item.setToolTip("Step matches ROP value.")
            elif col == 0:
                item.setToolTip(job.runtime.log_file_path or "")
            elif col == 16:
                item.setToolTip(job.view.out_path or "")
        self._style_row(row, job)

    def _refresh_job_row(self, job_id: str) -> None:
        target_id = str(job_id or "").strip()
        if not target_id:
            return
        row = next((idx for idx, job in enumerate(self.jobs) if job.id == target_id), -1)
        if row < 0 or row >= self.queue_table.rowCount():
            self._refresh_queue_table(select_job_id=target_id)
            return
        if self._queue_refresh_should_defer():
            self._pending_queue_refresh_args = {
                "select_row": None,
                "select_job_id": target_id,
                "select_job_ids": None,
            }
            self._pending_queue_refresh_timer.start(200)
            return
        table_blocker = QtCore.QSignalBlocker(self.queue_table)
        try:
            self._suppress_queue_item_changed = True
            self.queue_table.setUpdatesEnabled(False)
            self._populate_queue_row(row, self.jobs[row])
        finally:
            self.queue_table.setUpdatesEnabled(True)
            self._suppress_queue_item_changed = False
        self.queue_table.viewport().update()
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
            refresh_queue_tree_model(tree, model, self.jobs)
        finally:
            self._suppress_tree_item_changed = False

    def _style_row(self, row: int, job: RenderJob) -> None:
        color = None
        text_color = None
        t = {**DEFAULT_THEME, **getattr(self, "theme", {})}
        pal = self.queue_table.palette()
        base_color = pal.color(QtGui.QPalette.ColorRole.AlternateBase if row % 2 else QtGui.QPalette.ColorRole.Base)
        default_text_color = pal.color(QtGui.QPalette.ColorRole.Text)
        if not job.spec.enabled:
            color = QtGui.QColor("#161616")
            text_color = QtGui.QColor("#6f6f6f")
        elif job.runtime.status == JobStatus.OFFLINE:
            color = QtGui.QColor("#2f2f2f")
            text_color = QtGui.QColor("#b0b0b0")
        elif job.runtime.status == JobStatus.RUNNING:
            color = QtGui.QColor(t["queue_running"])
            text_color = QtGui.QColor("#ffffff")
        elif job.runtime.status == JobStatus.DONE:
            color = QtGui.QColor(t["queue_done"])
            text_color = QtGui.QColor("#ffffff")
        elif job.runtime.status == JobStatus.FAILED:
            color = QtGui.QColor(t["queue_failed"])
            text_color = QtGui.QColor("#ffffff")
        elif job.runtime.status == JobStatus.INTERRUPTED:
            color = QtGui.QColor("#6b4e16")
            text_color = QtGui.QColor("#ffffff")
        elif job.runtime.status == JobStatus.CANCELED:
            color = None
        for col in range(self.queue_table.columnCount()):
            item = self.queue_table.item(row, col)
            if item is not None:
                if job.runtime.status == JobStatus.OFFLINE:
                    item.setBackground(QtGui.QBrush(QtGui.QColor("#2f2f2f")))
                else:
                    item.setBackground(QtGui.QBrush(color if color else base_color))
                item.setForeground(QtGui.QBrush(text_color if text_color else default_text_color))

    def _append_log(self, source: str, text: str) -> None:
        if not text:
            return
        self.log_entries.append((source, text))
        self._append_to_log_view_if_matches(source, text)
        self._append_notifications(source, text)

    def _append_notifications(self, source: str, text: str) -> None:
        if not hasattr(self, "notifications_list") or self.notifications_list is None:
            return
        source_label = str(source or "").strip() or "Info"
        added = False
        for message, severity in self._notification_messages_for_log(source_label, text):
            item = QtWidgets.QListWidgetItem(message)
            item.setIcon(self._notification_icon_for_severity(severity))
            item.setForeground(QtGui.QBrush(self._notification_color_for_severity(severity)))
            self.notifications_list.addItem(item)
            added = True
        if not added:
            return
        max_items = 250
        while self.notifications_list.count() > max_items:
            self.notifications_list.takeItem(0)
        if self.notifications_list.count() > 0:
            self.notifications_list.scrollToBottom()

    def _notification_messages_for_log(self, source: str, text: str) -> list[tuple[str, str]]:
        messages: list[tuple[str, str]] = []
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            summary = self._notification_summary_for_line(source, line)
            if summary is not None:
                messages.append(summary)
        return messages

    @staticmethod
    def _notification_summary_for_line(source: str, line: str) -> tuple[str, str] | None:
        low = line.lower()
        if line.startswith("===") and line.endswith("==="):
            inner = line.strip("=").strip()
            inner_low = inner.lower()
            if inner_low == "queue started":
                return ("Queue started.", "info")
            if inner_low == "queue complete":
                return ("Queue complete.", "info")
            if inner_low == "queue stopped":
                return ("Queue stopped.", "warning")
            if inner_low == "queue aborted":
                return ("Queue aborted.", "error")
            if inner_low.startswith("render start:"):
                job_name = inner.split(":", 1)[1].strip() if ":" in inner else inner
                return (f"Started render: {job_name}", "info")
            return None
        if line.startswith("[Recovery] "):
            return (line[len("[Recovery] ") :].strip(), "warning")
        if line.startswith("[Queue] Stop requested"):
            return ("Stopping queue after the current step.", "warning")
        if line.startswith("[Queue] Terminating current render process"):
            return ("Stopping the active render.", "warning")
        if line.startswith("[Queue] Force killing current render process"):
            return ("Force-stopped the active render.", "error")
        if line.startswith("[Queue] Resumed"):
            return ("Queue resumed.", "info")
        if line.startswith("[Queue] Pause requested"):
            return ("Queue will pause after the current job.", "warning")
        if line.startswith("[Scan] No likely render/output nodes matched"):
            return ("No likely render nodes were found. Showing all scanned nodes.", "warning")
        if line.startswith("[Retry] "):
            return ("Retrying the current render chunk after a worker failure.", "warning")
        if line.startswith("[Preflight] Failed"):
            return ("Render preflight failed.", "error")
        if line.startswith("[Queue] Failed to save queue"):
            return ("Failed to save the queue file.", "error")
        if line.startswith("[Queue] Failed to load queue"):
            return ("Failed to load the queue file.", "error")
        if line.startswith("[Queue] Failed to start render worker"):
            return ("Failed to start the render worker.", "error")
        if line.startswith("[Log] Failed to open log file"):
            return ("Failed to open the job log file.", "error")
        if "unresponsive" in low and "worker" in low:
            if "render worker" in low:
                return ("The render worker stopped responding.", "error")
            if "scan worker" in low:
                return ("The scan worker stopped responding.", "error")
            return ("A worker process stopped responding.", "error")
        if "worker exited unexpectedly" in low:
            if "render worker" in low:
                return ("The render worker exited unexpectedly.", "error")
            if "scan worker" in low:
                return ("The scan worker exited unexpectedly.", "error")
            return ("A worker process exited unexpectedly.", "error")
        if source.lower() == "stderr" and any(token in low for token in ("traceback", "error", "failed", "interrupted")):
            if "interrupted" in low:
                return ("A render was interrupted.", "warning")
            if "traceback" in low:
                return ("A technical error was reported. See Logs for details.", "error")
            return ("An error was reported. See Logs for details.", "error")
        return None

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
        sev = str(severity or "info").lower()
        if sev == "error":
            return QtGui.QColor("#d96b6b")
        if sev == "warning":
            return QtGui.QColor("#d4ad4a")
        return QtGui.QColor("#d8d8d8")

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
        if hasattr(self, "notifications_list") and self.notifications_list is not None:
            self.notifications_list.clear()

    def _on_queue_selection_changed(self) -> None:
        self.queue_table.viewport().update()
        self._refresh_ui_state()

    @staticmethod
    def _parse_percent_value(text: str) -> int | None:
        m = re.search(r"(\d{1,3})\s*%", str(text or ""))
        if not m:
            return None
        try:
            return max(0, min(100, int(m.group(1))))
        except Exception:
            return None

    def _queue_progress_split_values(self, job: RenderJob) -> tuple[int | None, int | None]:
        pct = self._parse_percent_value(job.view.percent_text)
        build_pct: int | None = None
        render_pct: int | None = None
        show_usd_build = (job.runtime.allframesatonce_enabled is True)

        if job.runtime.status == JobStatus.DONE:
            render_pct = 100
            if show_usd_build and job.view.usd_build_percent is not None:
                build_pct = job.view.usd_build_percent
            else:
                build_pct = 100 if show_usd_build else None
            return build_pct, render_pct

        if show_usd_build and job.view.phase_text == "USD Build":
            build_pct = job.view.usd_build_percent if job.view.usd_build_percent is not None else pct
            render_pct = 0
            return build_pct, render_pct
        if job.view.phase_text == "Render":
            render_pct = pct
            if show_usd_build and job.view.usd_build_percent is not None:
                build_pct = job.view.usd_build_percent
            else:
                build_pct = 100 if show_usd_build and job.view.build_pass_completed else None
            return build_pct, render_pct

        if pct is not None:
            render_pct = pct
        if show_usd_build and job.view.usd_build_percent is not None:
            build_pct = job.view.usd_build_percent
        return build_pct, render_pct

    def _queue_header_visual_order(self) -> list[int]:
        header = self.queue_table.horizontalHeader()
        return [header.logicalIndex(v) for v in range(self.queue_table.columnCount())]

    def _queue_hidden_columns_from_data(self, raw: Any) -> set[int]:
        result: set[int] = set()
        if not isinstance(raw, list):
            return result
        for v in raw:
            try:
                i = int(v)
            except Exception:
                continue
            if 0 <= i < self.queue_table.columnCount():
                result.add(i)
        return result

    def _queue_column_widths_from_data(self, raw: Any) -> dict[int, int]:
        result: dict[int, int] = {}
        if not isinstance(raw, dict):
            return result
        for key, value in raw.items():
            try:
                logical = int(key)
                width = int(value)
            except Exception:
                continue
            if 0 <= logical < self.queue_table.columnCount() and width > 8:
                result[logical] = width
        return result

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
        left_group = {0, 1, 2, 3, 4, 5, 6}
        for logical in range(self.queue_table.columnCount()):
            if self.queue_table.isColumnHidden(logical):
                continue
            visual = header.visualIndex(logical)
            if visual < 0:
                continue
            if logical in left_group and visual >= 6:
                return False
            if logical not in left_group and visual < 6:
                return False
        return True

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
            hdr_item = self.queue_table.horizontalHeaderItem(logical)
            label = hdr_item.text() if hdr_item is not None else f"Column {logical + 1}"
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

    def _on_queue_item_changed(self, item: QtWidgets.QTableWidgetItem) -> None:
        if self._suppress_queue_item_changed:
            return
        if item is None:
            return
        try:
            row = item.row()
            col = item.column()
            if col not in {0, 1, 2, 3, 4, 5}:
                return
            if not (0 <= row < len(self.jobs)):
                return
            job = self.jobs[row]
            selected_rows = self._selected_rows()
            if row not in selected_rows:
                selected_rows = [row]
            target_rows = selected_rows
            target_job_ids = [self.jobs[r].id for r in target_rows if 0 <= r < len(self.jobs)]
            before_states = self._job_states_for_ids(target_job_ids)
            if col == 5:
                new_mode = FrameHandlingMode.from_label(item.text())
                changed = False
                for target_row in target_rows:
                    target = self.jobs[target_row]
                    if self._is_active_job(target):
                        continue
                    if target.frame_handling_mode != new_mode:
                        target.frame_handling_mode = new_mode
                        changed = True
                if not changed:
                    self._refresh_queue_preserve_selection()
                    return
                after_states = self._job_states_for_ids(target_job_ids)
                self._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_job_ids,
                        "redo_select_job_ids": target_job_ids,
                    }
                )
                self._save_and_refresh_queue(select_job_ids=target_job_ids)
                return
            if col == 0:
                new_name = item.text().strip()
                changed = False
                for target_row in target_rows:
                    target = self.jobs[target_row]
                    if self._is_active_job(target):
                        continue
                    target.name = new_name
                    changed = True
                if not changed:
                    self._refresh_queue_preserve_selection()
                    return
                after_states = self._job_states_for_ids(target_job_ids)
                self._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_job_ids,
                        "redo_select_job_ids": target_job_ids,
                    }
                )
                self._save_and_refresh_queue(select_job_ids=target_job_ids)
                return
            if col in {1, 2}:
                try:
                    source_text = validate_queue_path_value_model(col, item.text())
                except ValueError as exc:
                    safe_message(self, "Invalid Path", str(exc))
                    self._refresh_queue_preserve_selection()
                    return
                old_hip = str(job.spec.hip_path or "").strip()
                old_rop = str(job.spec.rop_path or "").strip()
                if col == 1:
                    changed_ids = self._propagate_hip_path_change(old_hip, source_text)
                else:
                    changed_ids = self._propagate_rop_path_change(old_hip, old_rop, source_text)
                if not changed_ids:
                    self._refresh_queue_preserve_selection()
                    return
                after_states = self._job_states_for_ids(changed_ids)
                self._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_job_ids,
                        "redo_select_job_ids": self._selection_ids_for_refresh(changed_ids) or [],
                    }
                )
                self._save_and_refresh_queue(select_job_ids=self._selection_ids_for_refresh(changed_ids))
                return
            # Frame Range / Step bulk-edit uses the edited row values as the source payload.
            frame_item = self.queue_table.item(row, 3)
            step_item = self.queue_table.item(row, 4)
            frame_text = (frame_item.text() if frame_item is not None else "").strip()
            step_text = (step_item.text() if step_item is not None else "").strip()
            changed = False
            for target_row in target_rows:
                target = self.jobs[target_row]
                if self._is_active_job(target):
                    continue
                if target.strict_frame_range:
                    continue
                try:
                    target_frame_text = frame_text if col == 3 else self._queue_edit_frame_text_for_job(target)
                    target_step_text = step_text if col == 4 else self._queue_edit_step_text_for_job(target)
                    apply_queue_frame_override_text(target, target_frame_text, target_step_text)
                    changed = True
                except ValueError as exc:
                    safe_message(self, "Invalid Frame Override", str(exc))
                    self._refresh_queue_preserve_selection()
                    return
                except Exception as exc:
                    safe_message(self, "Error", f"Failed to update frame override: {exc}", traceback.format_exc())
                    self._refresh_queue_preserve_selection()
                    return
            if not changed:
                self._refresh_queue_preserve_selection()
                return
            self._push_history_command(
                {
                    "kind": "update_jobs",
                    "before": before_states,
                    "after": self._job_states_for_ids(target_job_ids),
                    "undo_select_job_ids": target_job_ids,
                    "redo_select_job_ids": target_job_ids,
                }
            )
            self._save_and_refresh_queue(select_job_ids=target_job_ids)
        except Exception as exc:
            safe_message(self, "Queue Edit Error", f"Failed to apply queue edit: {exc}", traceback.format_exc())
            self._refresh_queue_preserve_selection()

    def _on_queue_frame_handling_chosen(self, row: int, text: str) -> None:
        if not (0 <= row < len(self.jobs)):
            return
        new_mode = FrameHandlingMode.from_label(text)
        selected_rows_before = self._selected_rows()
        selected_job_ids_before = [self.jobs[r].id for r in selected_rows_before if 0 <= r < len(self.jobs)]
        if row in selected_rows_before:
            target_rows = [r for r in selected_rows_before if 0 <= r < len(self.jobs)]
            preserve_job_ids = [self.jobs[r].id for r in target_rows]
        else:
            target_rows = [row]
            preserve_job_ids = selected_job_ids_before
        target_job_ids = [self.jobs[r].id for r in target_rows]
        before_states = self._job_states_for_ids(target_job_ids)
        changed = False
        for target_row in target_rows:
            target = self.jobs[target_row]
            if self._is_active_job(target):
                continue
            if target.spec.frame_handling_mode != new_mode:
                target.spec.frame_handling_mode = new_mode
                changed = True
        if changed:
            after_states = self._job_states_for_ids(target_job_ids)
            self._push_history_command(
                {
                    "kind": "update_jobs",
                    "before": before_states,
                    "after": after_states,
                    "undo_select_job_ids": preserve_job_ids or [],
                    "redo_select_job_ids": preserve_job_ids or [],
                }
            )
            self._save_and_refresh_queue(select_job_ids=preserve_job_ids or None)
            return
        self._save_and_refresh_queue(select_job_ids=preserve_job_ids or None)

    def _handle_scan_requested(self, request: dict) -> None:
        self.scan_coordinator.handle_scan_requested(request)
        self._refresh_ui_state()

    def _handle_scan_worker_message(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("request_id", "") or "")
        if request_id and self._active_scan_request_id and request_id != self._active_scan_request_id:
            return
        payload = dict(message.get("payload", {}) or {})
        message_type = str(message.get("type", "") or "")
        if message_type == "scan.result":
            self._active_scan_request_id = ""
            records = list(payload.get("records", []) or [])
            renderable_records = [r for r in records if self._is_likely_renderable_scan_node(r)]
            selected_records = renderable_records or records
            hip_path = str(payload.get("hip_path", "") or getattr(self, "_scan_hip_path_requested", "") or "")
            self._apply_scan_metadata_to_existing_jobs(hip_path, records)
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
            message_text = str(payload.get("message", "") or "Scan failed.")
            details = str(payload.get("stderr", "") or self.scan_worker_client.last_stderr_text or "")
            safe_message(self, "Scan", message_text, details or None)
            self._set_status_message(message_text, 5000)
            self._refresh_ui_state()

    def _apply_scan_metadata_to_existing_jobs(self, hip_path: str, records: list[dict[str, Any]]) -> None:
        hip_path_norm = os.path.normcase(str(hip_path or "").strip())
        if not hip_path_norm:
            return
        rec_map = {
            str(r.get("path", "")).strip(): r
            for r in records
            if str(r.get("path", "")).strip()
        }
        changed = False
        for job in self.jobs:
            if os.path.normcase(job.spec.hip_path) != hip_path_norm:
                continue
            rec = rec_map.get(job.spec.rop_path)
            if rec is None:
                continue
            before = (job.spec.strict_frame_range, job.view.out_path, job.view.out_file_sample_path)
            info = rop_info_from_scan_record_model(rec)
            apply_rop_info_to_job_model(
                job,
                info,
                self._normalize_output_display_path,
                apply_runtime_range=True,
            )
            after = (job.spec.strict_frame_range, job.view.out_path, job.view.out_file_sample_path)
            if before != after:
                changed = True
        if changed:
            self._save_queue_state()
            self._refresh_queue_table()

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

    def _start_queue(self) -> None:
        if self.queue_active:
            if self.queue_paused:
                self.queue_paused = False
                self._append_log("Stdout", "\n[Queue] Resumed\n")
                self._set_status_message("Queue resumed", 3000)
                self._maybe_start_next_job()
                self._refresh_ui_state()
            return
        if not self._hbatch_exists():
            safe_message(self, "hbatch Missing", "Configure a valid hbatch.exe path before starting the queue.")
            return
        selected = self._selected_job()
        can_start_selected = self._is_job_runnable(selected)
        has_runnable = any(self._is_job_runnable(job) for job in self.jobs)
        if not has_runnable and not can_start_selected:
            safe_message(self, "Queue Empty", "No enabled jobs to run.")
            return
        self.queue_active = True
        self.queue_paused = False
        self.stop_requested = False
        self.canceling_current_job = False
        self._queue_rerun_statuses = set()
        self._jobs_started_this_run = set()
        self._queue_next_search_index = 0
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
        self.queue_paused = not self.queue_paused
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
        self.stop_requested = True
        self.queue_paused = False
        self._append_log("Stdout", "\n[Queue] Stop requested\n")
        self._set_status_message("Stopping queue...")
        if self._render_job_active():
            self.canceling_current_job = True
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
        if not self.queue_active or self.queue_paused or self._render_job_active():
            return
        if self.stop_requested:
            self._finish_queue("Queue stopped")
            return
        next_job = select_next_runnable_job(
            self.jobs,
            start_index=self._queue_next_search_index,
            is_runnable=self._is_job_runnable,
            started_job_ids=self._jobs_started_this_run,
        )
        if next_job is None:
            self._finish_queue("Queue complete")
            return
        self._start_job(next_job)

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

        self._clear_job_resume_runtime_state(job)
        frame_plan = plan_frame_handling_model(
            job,
            overwrite_mode=FrameHandlingMode.OVERWRITE,
            render_missing_mode=FrameHandlingMode.RENDER_MISSING,
            render_from_first_missing_mode=FrameHandlingMode.RENDER_FROM_FIRST_MISSING,
            compute_resume_from_output=lambda target: self._compute_resume_from_output(target, interactive=False),
            compute_missing_ranges_from_output=lambda target: self._compute_missing_ranges_from_output(target, interactive=False),
        )
        baseline_done = int(frame_plan.baseline_done)
        forced_ranges = frame_plan.forced_ranges
        if frame_plan.info_message:
            self._append_log("Stdout", frame_plan.info_message)
        if frame_plan.already_complete:
            mark_job_done_without_render_model(job, done_status=JobStatus.DONE, now_fn=datetime.now)
            self._save_queue_state()
            self._refresh_queue_table(select_job_id=job.id)
            self._maybe_start_next_job()
            return

        # Build runtime chunk plan after frame-handling planning.
        self._initialize_job_chunk_runtime(job, forced_ranges=forced_ranges)
        if not job.runtime.chunk_ranges_runtime and self._chunking_enabled():
            # Fall back to non-chunked execution if range resolution failed.
            job.runtime.chunk_retry_count_runtime = self._retry_count_value()

        resume_baseline = int(max(0, baseline_done))

        job.runtime.status = JobStatus.RUNNING
        job.runtime.started_at = datetime.now()
        job.runtime.finished_at = None
        job.runtime.exit_code = None
        job.runtime.error_summary = ""
        job.runtime.offline_detected_reason = ""
        self._reset_job_process_attempt_state(job)
        job.runtime.runtime_start_frame = None
        job.runtime.runtime_end_frame = None
        job.runtime.runtime_step = None
        # Keep the last known output path visible until a newer one is detected.
        job.runtime.resume_completed_baseline_count = resume_baseline
        self._cancel_phase_promote()
        self.current_job_id = job.id
        self.canceling_current_job = False
        self._jobs_started_this_run.add(job.id)
        try:
            self._queue_next_search_index = self.jobs.index(job) + 1
        except ValueError:
            self._queue_next_search_index = 0

        try:
            Path(job.runtime.log_file_path).parent.mkdir(parents=True, exist_ok=True)
            self.current_job_log_handle = open(job.runtime.log_file_path, "a", encoding="utf-8", errors="replace")
        except Exception as exc:
            self.current_job_log_handle = None
            self._append_log("Stderr", f"[Log] Failed to open log file: {exc}\n")

        self._write_job_log(f"=== Job Started: {job.display_name()} ===\n")
        self._write_job_log(f"HIP: {job.spec.hip_path}\nROP: {job.spec.rop_path}\nFrames: {job.frame_display()}\n")
        if job.runtime.chunk_total_runtime > 1:
            self._write_job_log(
                f"[Chunking] {job.runtime.chunk_total_runtime} chunks | retries={job.runtime.chunk_retry_count_runtime} | delay={self._retry_delay_value()}s\n"
            )
        if resume_baseline > 0:
            self._write_job_log(f"[Frame Handling] Existing frames baseline: {resume_baseline}\n")

        self._append_log("Stdout", f"\n=== Render Start: {job.display_name()} ===\n")
        if job.runtime.chunk_total_runtime > 1:
            self._append_log(
                "Stdout",
                f"[Chunk] {job.runtime.chunk_index_runtime + 1}/{job.runtime.chunk_total_runtime} | frames {job.runtime.chunk_start_frame_runtime}-{job.runtime.chunk_end_frame_runtime} | attempt {job.runtime.chunk_attempt_runtime}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}\n",
            )
        payload = self.render_session.build_render_worker_payload(job)
        if payload is None or not self._start_render_worker_payload(payload):
            self._append_log("Stderr", "[Queue] Failed to start render worker.\n")
            job.runtime.status = JobStatus.FAILED
            job.runtime.exit_code = -1
            job.runtime.finished_at = datetime.now()
            job.runtime.error_summary = "Failed to start render worker."
            self._close_current_job_log()
            self.current_job_id = None
            self._refresh_queue_table(select_job_id=job.id)
            self._maybe_start_next_job()
            return

        self._refresh_queue_table(select_job_id=job.id)
        self._set_status_message(f"Running: {job.display_name()}")

    def _send_render_commands(self) -> None:
        return

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
        request_id = str(message.get("request_id", "") or "")
        if request_id and self._active_render_request_id and request_id != self._active_render_request_id:
            return
        payload = dict(message.get("payload", {}) or {})
        message_type = str(message.get("type", "") or "")
        if message_type == "render.started":
            self._render_finished_message_received = False
            try:
                self._active_hbatch_pid = int(payload.get("pid", 0) or 0)
            except Exception:
                self._active_hbatch_pid = 0
            return
        if message_type == "render.output":
            text = str(payload.get("text", "") or "")
            stream = str(payload.get("stream", "stdout") or "stdout")
            if not text:
                return
            self._append_log("Stderr" if stream == "stderr" else "Stdout", text)
            self._write_job_log(text)
            self._update_job_progress_from_output(text)
            return
        if message_type == "render.finished":
            self._render_finished_message_received = True
            self._active_render_request_id = ""
            normal_exit_value = int(
                getattr(QtCore.QProcess.ExitStatus.NormalExit, "value", QtCore.QProcess.ExitStatus.NormalExit)
            )
            self._on_render_finished(
                int(payload.get("exit_code", -1)),
                QtCore.QProcess.ExitStatus(int(payload.get("exit_status", normal_exit_value))),
            )
            return
        if message_type == "render.crashed":
            reason = str(payload.get("reason", "") or "Render worker reported a crash.")
            process_error = payload.get("process_error")
            suffix = f" ({process_error})" if process_error not in (None, "") else ""
            self._handle_render_worker_crash(f"{reason}{suffix}")

    def _handle_render_worker_crash(self, reason: str) -> None:
        if self._pending_kill_timer is not None:
            self._pending_kill_timer.stop()
        self._kill_active_hbatch_tree()
        job = self._current_job()
        if job is None:
            return
        crash_result = self.render_session.handle_worker_crash(
            job,
            reason,
            canceling_current_job=self.canceling_current_job,
            stop_requested=self.stop_requested,
            retry_delay_value=self._retry_delay_value(),
        )
        if crash_result.terminal_finish:
            if not self.canceling_current_job and not self.stop_requested:
                self.render_session.finalize_worker_crash(job, reason)
                finished_job_id = job.id
                try:
                    self._queue_next_search_index = self.jobs.index(job) + 1
                except ValueError:
                    self._queue_next_search_index = 0
                self.current_job_id = None
                self._active_hbatch_pid = 0
                self.canceling_current_job = False
                self._close_current_job_log()
                self._save_queue_state()
                self._refresh_queue_table(select_job_id=finished_job_id)
                self._maybe_start_next_job()
                return
            self._on_render_finished(-1, QtCore.QProcess.ExitStatus.CrashExit)
            return
        if crash_result.retry_scheduled:
            self._reset_job_process_attempt_state(job)
            if crash_result.delay_ms > 0:
                QtCore.QTimer.singleShot(crash_result.delay_ms, lambda j=job: self._start_job_process_continuation(j))
            else:
                self._start_job_process_continuation(job)
        return

    def _update_job_progress_from_output(self, text: str) -> None:
        job = self._current_job()
        if job is None:
            return
        self.render_session.handle_worker_output(job, text)

    @staticmethod
    def _total_frames_for_job(job: RenderJob) -> int | None:
        return total_frames_for_job_model(job)

    @staticmethod
    def _update_job_render_timing_stats(job: RenderJob) -> None:
        update_job_render_timing_stats_model(job, format_duration_short_fn=format_duration_short)

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
        if self._pending_kill_timer is not None:
            self._pending_kill_timer.stop()
        self._active_hbatch_pid = 0
        job = self._current_job()
        if job is None:
            return

        was_canceled = bool(self.canceling_current_job or self.stop_requested)
        finish_result = self.render_session.handle_render_finished(
            job,
            exit_code,
            exit_status,
            was_canceled=was_canceled,
            advance_job_to_next_chunk=self._advance_job_to_next_chunk,
            retry_delay_value=self._retry_delay_value(),
        )
        self._active_render_request_id = ""
        if finish_result.continue_next_chunk:
            self._reset_job_process_attempt_state(job)
            self._start_job_process_continuation(job)
            return
        if finish_result.retry_current_chunk:
            self._reset_job_process_attempt_state(job)
            if finish_result.delay_ms > 0:
                QtCore.QTimer.singleShot(finish_result.delay_ms, lambda j=job: self._start_job_process_continuation(j))
            else:
                self._start_job_process_continuation(job)
            return
        self._clear_job_resume_runtime_state(job)
        job.chunk_start_frame_runtime = None
        job.chunk_end_frame_runtime = None
        job.chunk_step_runtime = None
        job.chunk_ranges_runtime.clear()
        job.chunk_index_runtime = 0
        job.chunk_total_runtime = 0
        job.chunk_attempt_runtime = 0
        finished_job_id = job.id
        try:
            self._queue_next_search_index = self.jobs.index(job) + 1
        except ValueError:
            self._queue_next_search_index = 0
        self.current_job_id = None
        self.canceling_current_job = False

        if self.stop_requested:
            self._save_queue_state()
            self._finish_queue("Queue stopped")
        else:
            self._save_queue_state()
            self._refresh_queue_table(select_job_id=finished_job_id)
            self._maybe_start_next_job()

    def _finish_queue(self, message: str) -> None:
        self.queue_active = False
        self.queue_paused = False
        self.stop_requested = False
        self.current_job_id = None
        self._active_hbatch_pid = 0
        self.canceling_current_job = False
        self._queue_rerun_statuses = set()
        self._jobs_started_this_run = set()
        self._queue_next_search_index = 0
        self._set_status_message(message, 5000)
        self._append_log("Stdout", f"\n=== {message} ===\n")
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
        except Exception:
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
        except Exception:
            pass

    def _close_current_job_log(self) -> None:
        try:
            if self.current_job_log_handle is not None:
                self.current_job_log_handle.close()
        except Exception:
            pass
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
        if preview_path is None:
            safe_message(self, "Preview", "No previewable output path is available for this job.")
            return
        player_path = self._current_player_path()
        if not player_path:
            safe_message(self, "Preview", "Configure a preview player path in Preferences first.")
            return
        player = Path(player_path)
        if not player.exists():
            safe_message(self, "Preview", f"Preview player does not exist:\n{player}")
            return
        try:
            started = QtCore.QProcess.startDetached(str(player), [str(preview_path)])
        except Exception as exc:
            safe_message(self, "Preview", f"Failed to launch preview player:\n{player}", str(exc))
            return
        if not started:
            safe_message(self, "Preview", f"Failed to launch preview player:\n{player}")

    def _open_logs_folder(self) -> None:
        folder = self.config.logs_dir
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            safe_message(self, "Logs Folder", f"Failed to create logs folder:\n{folder}", str(exc))
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))

    def _clear_log_files(self) -> None:
        if self.current_job_log_handle is not None or self._render_job_active():
            safe_message(self, "Logs Busy", "Cannot delete log files while a render is active.")
            return
        logs_dir = self.config.logs_dir
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            safe_message(self, "Logs Folder", f"Failed to access logs folder:\n{logs_dir}", str(exc))
            return

        log_paths = sorted(p for p in logs_dir.glob("*.log") if p.is_file())
        if not log_paths:
            safe_message(self, "Logs", "No log files found.")
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
            except Exception as exc:
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
        if not folder.exists():
            safe_message(self, "Folder Missing", f"Folder does not exist:\n{folder}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))

    def _refresh_ui_state(self) -> None:
        running = self.queue_active and self._render_job_active()
        scan_in_progress = self._scan_in_progress()
        hbatch_ok = self._hbatch_exists()

        self.add_job_panel.set_enabled_for_run_state(self.queue_active, scan_in_progress)
        self.btn_preferences.setEnabled(not scan_in_progress)

        has_queued = any(self._is_job_runnable(j) for j in self.jobs)
        selected = self._selected_job()
        can_start_selected = self._is_job_runnable(selected)
        self.btn_start_queue.setEnabled(
            hbatch_ok and (has_queued or can_start_selected or (self.queue_active and self.queue_paused))
        )
        self.btn_pause_queue.setEnabled(self.queue_active)
        self.btn_pause_queue.setText("Resume" if self.queue_paused else "Pause")
        self.btn_stop_queue.setEnabled(self.queue_active or self._render_job_active())
        self.queue_file_menu_button.setEnabled(not scan_in_progress and not self._render_job_active())
        self.chk_disable_husk_mplay.setEnabled(not self.queue_active and not self._render_job_active())
        self.chk_enable_chunking.setEnabled(not self.queue_active and not self._render_job_active())
        self.spin_chunk_size.setEnabled(
            not self.queue_active and not self._render_job_active() and self.chk_enable_chunking.isChecked()
        )
        self.spin_auto_retry.setEnabled(not self.queue_active and not self._render_job_active())
        self.spin_retry_delay.setEnabled(not self.queue_active and not self._render_job_active())

        selected = self._selected_job()
        self.btn_open_log_file.setEnabled(bool(selected and selected.log_file_path))

        if running:
            self._set_status_message("Rendering...")
        elif self.queue_active and self.queue_paused:
            self._set_status_message("Queue paused")
        elif scan_in_progress:
            self._set_status_message("Scanning /out ...")

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
        except Exception:
            pass
        sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _hook


def create_app() -> QtWidgets.QApplication:
    QtCore.QCoreApplication.setOrganizationName(ORG_NAME)
    QtCore.QCoreApplication.setApplicationName(APP_NAME)
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    return app


def main() -> int:
    install_excepthook()
    app = create_app()
    win = MainWindow()
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

