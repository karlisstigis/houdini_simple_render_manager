from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from PySide6 import QtCore, QtGui, QtWidgets

from action_policy import can_open_queue_file


@dataclass
class QueueFileControllerHooks:
    config_get: Callable[[str, object | None], object | None]
    config_set: Callable[[str, object], None]
    default_queue_path: Callable[[], Path]
    base_dir_path: Callable[[], Path]
    queue_active: Callable[[], bool]
    render_job_active: Callable[[], bool]
    scan_in_progress: Callable[[], bool]
    safe_message: Callable[[str, str, str | None], None]
    load_queue_from_path: Callable[[Path], bool]
    save_queue_state: Callable[[Path | None], bool]
    set_status_message: Callable[[str, int | None], None]
    set_window_title: Callable[[str], None]


class QueueFileController:
    def __init__(self, app_name: str, hooks: QueueFileControllerHooks) -> None:
        self._app_name = app_name
        self._hooks = hooks

    def current_queue_file_path(self) -> Path:
        configured = str(self._hooks.config_get("last_queue_path", "") or "").strip()
        if configured:
            path = Path(configured)
            if path.exists():
                return path
        return self._hooks.default_queue_path()

    def set_current_queue_file_path(self, path: Path) -> None:
        self._hooks.config_set("last_queue_path", str(path))
        self.update_window_title()

    def update_window_title(self) -> None:
        queue_path = self.current_queue_file_path()
        default_path = self._hooks.default_queue_path()
        try:
            is_default = queue_path.resolve() == default_path.resolve()
        except Exception:
            is_default = queue_path == default_path
        self._hooks.set_window_title(self._app_name if is_default else f"{self._app_name} - {queue_path.name}")

    def queue_file_dialog_start_dir(self) -> str:
        current = self.current_queue_file_path()
        parent = current.parent if current.parent else self._hooks.base_dir_path()
        return str(parent)

    def open_queue_file_dialog(self, parent: QtWidgets.QWidget) -> None:
        decision = can_open_queue_file(
            queue_active=self._hooks.queue_active(),
            render_job_active=self._hooks.render_job_active(),
            scan_in_progress=self._hooks.scan_in_progress(),
        )
        if not decision.allowed:
            self._hooks.safe_message("Queue Busy", decision.reason, None)
            return
        path_text, _ = QtWidgets.QFileDialog.getOpenFileName(
            parent,
            "Open Queue",
            self.queue_file_dialog_start_dir(),
            "Queue Files (*.json);;All Files (*.*)",
        )
        if not path_text:
            return
        path = Path(path_text)
        if self._hooks.load_queue_from_path(path):
            self._hooks.set_status_message(f"Queue opened: {path}", 4000)

    def save_queue_as_dialog(self, parent: QtWidgets.QWidget) -> None:
        path_text, _ = QtWidgets.QFileDialog.getSaveFileName(
            parent,
            "Save Queue As",
            str(self.current_queue_file_path()),
            "Queue Files (*.json);;All Files (*.*)",
        )
        if not path_text:
            return
        path = Path(path_text)
        if path.suffix == "":
            path = path.with_suffix(".json")
        if self._hooks.save_queue_state(path):
            self._hooks.set_status_message(f"Queue saved: {path}", 4000)

    def open_current_queue_folder(self, parent: QtWidgets.QWidget) -> None:
        folder = self.current_queue_file_path().parent
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            self._hooks.safe_message("Queue Folder", f"Failed to access queue folder:\n{folder}", str(exc))
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))
