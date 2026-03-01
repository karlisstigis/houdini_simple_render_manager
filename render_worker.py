from __future__ import annotations

import subprocess
import sys

from PySide6 import QtCore

from worker_protocol import MessageBuffer, StdinReader, encode_message


class RenderWorker(QtCore.QObject):
    def __init__(self) -> None:
        super().__init__()
        self._buffer = MessageBuffer()
        self._process: QtCore.QProcess | None = None
        self._active_request_id = ""
        self._active_job_id = ""
        self._graceful_stop_requested = False
        self._stdin_reader = StdinReader(self)
        self._stdin_reader.chunk_read.connect(self._consume_stdin_chunk)
        self._stdin_reader.closed.connect(self._handle_stdin_closed)
        self._stdin_reader.start()

    def _emit(self, message_type: str, request_id: str, payload: dict | None = None) -> None:
        sys.stdout.buffer.write(encode_message(message_type, request_id, payload or {}))
        sys.stdout.buffer.flush()

    def _consume_stdin_chunk(self, chunk: bytes) -> None:
        for message in self._buffer.push_bytes(chunk):
            self._handle_message(message)

    def _handle_stdin_closed(self) -> None:
        self._shutdown()
        QtCore.QCoreApplication.quit()

    def _handle_message(self, message: dict) -> None:
        request_id = str(message.get("request_id", "") or "")
        message_type = str(message.get("type", "") or "")
        payload = dict(message.get("payload", {}) or {})
        if message_type == "render.start":
            self._handle_render_start(request_id, payload)
            return
        if message_type == "render.stop":
            self._handle_render_stop(request_id)
            return
        if message_type == "render.kill":
            self._handle_render_kill(request_id)
            return
        self._emit("worker.error", request_id, {"message": f"Unknown message type: {message_type}"})

    def _handle_render_start(self, request_id: str, payload: dict) -> None:
        if self._process is not None:
            self._emit("render.crashed", request_id, {"job_id": self._active_job_id, "reason": "Render worker is busy.", "process_error": "busy", "last_known_state": {}})
            return
        job_id = str(payload.get("job_id", "") or "").strip()
        hip_path = str(payload.get("hip_path", "") or "").strip()
        hbatch_path = str(payload.get("hbatch_path", "") or "").strip()
        commands = [str(cmd) for cmd in list(payload.get("commands", []) or []) if str(cmd).strip()]
        effective_plan = dict(payload.get("effective_plan", {}) or {})
        if not job_id or not hip_path or not hbatch_path or not commands:
            self._emit("render.crashed", request_id, {"job_id": job_id, "reason": "Invalid render payload.", "process_error": "invalid_payload", "last_known_state": {}})
            return

        proc = QtCore.QProcess(self)
        self._process = proc
        self._active_request_id = request_id
        self._active_job_id = job_id
        self._graceful_stop_requested = False

        proc.setProcessChannelMode(QtCore.QProcess.ProcessChannelMode.SeparateChannels)
        proc.readyReadStandardOutput.connect(self._on_stdout)
        proc.readyReadStandardError.connect(self._on_stderr)
        proc.errorOccurred.connect(self._on_process_error)
        proc.finished.connect(self._on_finished)
        proc.started.connect(lambda: self._on_started(commands, effective_plan))

        proc.start(hbatch_path, [hip_path])
        if not proc.waitForStarted(5000):
            self._emit("render.crashed", request_id, {"job_id": job_id, "reason": "Failed to start hbatch.", "process_error": "failed_to_start", "last_known_state": {}})
            self._clear_process()

    def _on_started(self, commands: list[str], effective_plan: dict) -> None:
        if self._process is None:
            return
        self._emit(
            "render.started",
            self._active_request_id,
            {
                "job_id": self._active_job_id,
                "pid": int(self._process.processId()),
                "effective_plan": effective_plan,
            },
        )
        self._process.write(("\n".join(commands) + "\n").encode("utf-8", errors="replace"))
        self._process.closeWriteChannel()

    def _on_stdout(self) -> None:
        if self._process is None:
            return
        text = bytes(self._process.readAllStandardOutput()).decode(errors="replace")
        if text:
            self._emit("render.output", self._active_request_id, {"job_id": self._active_job_id, "stream": "stdout", "text": text})

    def _on_stderr(self) -> None:
        if self._process is None:
            return
        text = bytes(self._process.readAllStandardError()).decode(errors="replace")
        if text:
            self._emit("render.output", self._active_request_id, {"job_id": self._active_job_id, "stream": "stderr", "text": text})

    def _on_process_error(self, err: QtCore.QProcess.ProcessError) -> None:
        self._emit(
            "render.output",
            self._active_request_id,
            {
                "job_id": self._active_job_id,
                "stream": "stderr",
                "text": f"[Render Worker] hbatch process error: {int(err)}\n",
            },
        )

    def _on_finished(self, exit_code: int, exit_status: QtCore.QProcess.ExitStatus) -> None:
        exit_status_value = int(getattr(exit_status, "value", exit_status))
        self._emit(
            "render.finished",
            self._active_request_id,
            {
                "job_id": self._active_job_id,
                "exit_code": int(exit_code),
                "exit_status": exit_status_value,
                "logical_success": bool(exit_code == 0 and exit_status == QtCore.QProcess.ExitStatus.NormalExit),
                "status": "finished",
                "error_summary": "",
                "final_job_state": {},
                "was_stopped": bool(self._graceful_stop_requested),
            },
        )
        self._clear_process()

    def _handle_render_stop(self, request_id: str) -> None:
        _ = request_id
        if self._process is None:
            return
        self._graceful_stop_requested = True
        self._process.terminate()

    def _handle_render_kill(self, request_id: str) -> None:
        _ = request_id
        if self._process is None:
            return
        if not self._kill_process_tree():
            self._process.kill()

    def _kill_process_tree(self) -> bool:
        if self._process is None:
            return False
        try:
            pid = int(self._process.processId())
        except Exception:
            return False
        if pid <= 0:
            return False
        if sys.platform.startswith("win"):
            try:
                result = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                output = f"{result.stdout}\n{result.stderr}".lower()
                return result.returncode == 0 or "not found" in output or "no running instance" in output
            except Exception:
                return False
        return False

    def _shutdown(self) -> None:
        if self._process is None:
            return
        self._graceful_stop_requested = True
        if not self._kill_process_tree():
            self._process.kill()
            self._process.waitForFinished(2000)
        self._clear_process()

    def _clear_process(self) -> None:
        if self._process is not None:
            self._process.deleteLater()
        self._process = None
        self._active_request_id = ""
        self._active_job_id = ""
        self._graceful_stop_requested = False
def main() -> int:
    app = QtCore.QCoreApplication(sys.argv)
    _worker = RenderWorker()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
