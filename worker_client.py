from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import uuid4

from PySide6 import QtCore

from worker_protocol import MessageBuffer, encode_message


@dataclass
class WorkerEvent:
    source: Literal["scan", "render"]
    message: dict


class _BaseWorkerClient(QtCore.QObject):
    message_received = QtCore.Signal(dict)
    stderr_received = QtCore.Signal(str)
    worker_failed = QtCore.Signal(str)
    busy_changed = QtCore.Signal(bool)

    def __init__(
        self,
        *,
        worker_python_path: str,
        worker_script_path: Path,
        terminal_message_types: set[str],
        tracked_request_types: set[str],
        parent: QtCore.QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._worker_python_path = str(worker_python_path or "python")
        self._worker_script_path = Path(worker_script_path)
        self._terminal_message_types = set(terminal_message_types)
        self._tracked_request_types = set(tracked_request_types)
        self._process: QtCore.QProcess | None = None
        self._stdout_buffer = MessageBuffer()
        self._last_stderr_text = ""
        self._active_request_id = ""

    @property
    def last_stderr_text(self) -> str:
        return self._last_stderr_text

    def process(self) -> QtCore.QProcess | None:
        return self._process

    def is_busy(self) -> bool:
        return bool(self._active_request_id)

    def ensure_started(self) -> bool:
        if self._process is not None and self._process.state() != QtCore.QProcess.ProcessState.NotRunning:
            return True
        proc = QtCore.QProcess(self)
        self._process = proc
        self._last_stderr_text = ""
        proc.setProcessChannelMode(QtCore.QProcess.ProcessChannelMode.SeparateChannels)
        proc.readyReadStandardOutput.connect(self._on_stdout)
        proc.readyReadStandardError.connect(self._on_stderr)
        proc.errorOccurred.connect(self._on_process_error)
        proc.finished.connect(self._on_finished)
        proc.start(self._worker_python_path, [str(self._worker_script_path)])
        if not proc.waitForStarted(5000):
            self._process = None
            return False
        return True

    def send_request(self, message_type: str, payload: dict, *, request_id: str | None = None) -> str | None:
        if not self.ensure_started() or self._process is None:
            return None
        send_request_id = str(request_id or uuid4().hex)
        if message_type in self._tracked_request_types:
            self._set_active_request_id(send_request_id)
        self._process.write(encode_message(message_type, send_request_id, payload))
        return send_request_id

    def shutdown(self) -> None:
        proc = self._process
        if proc is None:
            return
        if proc.state() != QtCore.QProcess.ProcessState.NotRunning:
            proc.kill()
            proc.waitForFinished(2000)
        proc.deleteLater()
        self._process = None
        self._set_active_request_id("")

    def _set_active_request_id(self, request_id: str) -> None:
        previous = bool(self._active_request_id)
        self._active_request_id = str(request_id or "")
        current = bool(self._active_request_id)
        if previous != current:
            self.busy_changed.emit(current)

    def _on_stdout(self) -> None:
        if self._process is None:
            return
        for message in self._stdout_buffer.push_bytes(bytes(self._process.readAllStandardOutput())):
            request_id = str(message.get("request_id", "") or "")
            message_type = str(message.get("type", "") or "")
            if request_id and request_id == self._active_request_id and message_type in self._terminal_message_types:
                self._set_active_request_id("")
            self.message_received.emit(message)

    def _on_stderr(self) -> None:
        if self._process is None:
            return
        text = bytes(self._process.readAllStandardError()).decode(errors="replace")
        if not text:
            return
        self._last_stderr_text += text
        self.stderr_received.emit(text)

    def _on_process_error(self, err: QtCore.QProcess.ProcessError) -> None:
        self.stderr_received.emit(f"[Worker process error] {err}\n")

    def _unexpected_exit_message(self, exit_code: int) -> str:
        return f"Worker exited unexpectedly (code {exit_code})."

    def _on_finished(self, exit_code: int, exit_status: QtCore.QProcess.ExitStatus) -> None:
        _ = exit_status
        proc = self._process
        if proc is not None:
            proc.deleteLater()
        self._process = None
        if self._active_request_id:
            self.worker_failed.emit(self._unexpected_exit_message(exit_code))
            self._set_active_request_id("")


class ScanWorkerClient(_BaseWorkerClient):
    def __init__(self, *, worker_python_path: str, worker_script_path: Path, parent: QtCore.QObject | None = None) -> None:
        super().__init__(
            worker_python_path=worker_python_path,
            worker_script_path=worker_script_path,
            terminal_message_types={"scan.result", "scan.failed", "probe.result", "probe.strict_range_result", "worker.error"},
            tracked_request_types={"scan.nodes", "scan.rop_info", "scan.strict_range"},
            parent=parent,
        )

    def request_sync(self, message_type: str, payload: dict, *, timeout_ms: int = 30000) -> dict | None:
        if self.is_busy():
            return None
        request_id = self.send_request(message_type, payload)
        if not request_id:
            return None
        result_holder: dict[str, dict] = {}
        loop = QtCore.QEventLoop(self)
        timer = QtCore.QTimer(self)
        timer.setSingleShot(True)

        def _finish_with(message: dict) -> None:
            result_holder["message"] = message
            if loop.isRunning():
                loop.quit()

        def _on_message(message: dict) -> None:
            if str(message.get("request_id", "") or "") == request_id:
                _finish_with(message)

        def _on_failure(reason: str) -> None:
            _finish_with(
                {
                    "type": "scan.failed",
                    "request_id": request_id,
                    "payload": {"message": reason, "stderr": self.last_stderr_text, "exit_code": -1},
                }
            )

        def _on_timeout() -> None:
            _finish_with(
                {
                    "type": "scan.failed",
                    "request_id": request_id,
                    "payload": {"message": "Timed out waiting for scan worker response.", "stderr": "", "exit_code": -1},
                }
            )

        self.message_received.connect(_on_message)
        self.worker_failed.connect(_on_failure)
        timer.timeout.connect(_on_timeout)
        timer.start(timeout_ms)
        loop.exec()
        timer.stop()
        try:
            self.message_received.disconnect(_on_message)
        except Exception:
            pass
        try:
            self.worker_failed.disconnect(_on_failure)
        except Exception:
            pass
        return result_holder.get("message")


class RenderWorkerClient(_BaseWorkerClient):
    def __init__(self, *, worker_python_path: str, worker_script_path: Path, parent: QtCore.QObject | None = None) -> None:
        super().__init__(
            worker_python_path=worker_python_path,
            worker_script_path=worker_script_path,
            terminal_message_types={"render.finished", "render.crashed", "worker.error"},
            tracked_request_types={"render.start"},
            parent=parent,
        )
