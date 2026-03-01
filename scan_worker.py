from __future__ import annotations

import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any

from PySide6 import QtCore

from houdini_service import ensure_range_probe_script, ensure_scan_script, parse_scan_output, probe_rop_info
from worker_protocol import MessageBuffer, StdinReader, encode_message


def hscript_quote(value: str) -> str:
    escaped = value.replace("\\", "/").replace('"', r"\"")
    return f'"{escaped}"'


class ScanWorker(QtCore.QObject):
    def __init__(self) -> None:
        super().__init__()
        self._buffer = MessageBuffer()
        self._busy = False
        self._stdin_reader = StdinReader(self)
        self._stdin_reader.chunk_read.connect(self._consume_stdin_chunk)
        self._stdin_reader.closed.connect(QtCore.QCoreApplication.quit)
        self._stdin_reader.start()

    def _emit(self, message_type: str, request_id: str, payload: dict[str, Any] | None = None) -> None:
        sys.stdout.buffer.write(encode_message(message_type, request_id, payload))
        sys.stdout.buffer.flush()

    def _consume_stdin_chunk(self, chunk: bytes) -> None:
        for message in self._buffer.push_bytes(chunk):
            self._handle_message(message)

    def _handle_message(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("request_id", "") or "")
        message_type = str(message.get("type", "") or "")
        payload = dict(message.get("payload", {}) or {})
        if message_type == "scan.nodes":
            self._handle_scan_nodes(request_id, payload)
            return
        if message_type == "scan.rop_info":
            self._handle_probe_rop_info(request_id, payload)
            return
        if message_type == "scan.strict_range":
            self._handle_probe_strict_range(request_id, payload)
            return
        self._emit("worker.error", request_id, {"message": f"Unknown message type: {message_type}"})

    def _handle_scan_nodes(self, request_id: str, payload: dict[str, Any]) -> None:
        if self._busy:
            self._emit("scan.failed", request_id, {"message": "Scan worker is busy.", "stderr": "", "exit_code": -1})
            return
        self._busy = True
        try:
            resolved = self._common_scan_paths_payload(request_id, payload)
            if resolved is None:
                return
            hip_path, hbatch_path, scripts_dir, hooks_dir = resolved
            roots = [str(root).strip() for root in list(payload.get("roots", []) or []) if str(root).strip()]
            if not roots:
                self._emit("scan.failed", request_id, {"message": "At least one scan root is required.", "stderr": "", "exit_code": -1})
                return
            scan_script_path = ensure_scan_script(
                scripts_dir=scripts_dir,
                hook_script_path_fn=lambda stem: hooks_dir / f"{stem}.py",
            )
            args = " ".join(hscript_quote(root) for root in roots)
            payload_text = f"python {hscript_quote(str(scan_script_path))} {args}\nquit\n"
            result = subprocess.run(
                [hbatch_path, hip_path],
                input=payload_text,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            combined = (result.stdout or "") + "\n" + (result.stderr or "")
            records = parse_scan_output(combined)
            if result.returncode != 0 and not records:
                self._emit(
                    "scan.failed",
                    request_id,
                    {"message": f"Scan failed (exit code {result.returncode}).", "stderr": combined, "exit_code": result.returncode},
                )
                return
            self._emit("scan.result", request_id, {"hip_path": hip_path, "records": records})
        except Exception as exc:
            self._emit(
                "scan.failed",
                request_id,
                {"message": str(exc), "stderr": traceback.format_exc(), "exit_code": -1},
            )
        finally:
            self._busy = False

    def _common_scan_paths_payload(self, request_id: str, payload: dict[str, Any]) -> tuple[str, str, Path, Path] | None:
        hip_path = str(payload.get("hip_path", "") or "").strip()
        hbatch_path = str(payload.get("hbatch_path", "") or "").strip()
        scripts_dir = Path(str(payload.get("scripts_dir", "") or "").strip())
        hooks_dir = Path(str(payload.get("hooks_dir", "") or "").strip())
        if not hip_path:
            self._emit("scan.failed", request_id, {"message": "HIP path is required.", "stderr": "", "exit_code": -1})
            return None
        if not hbatch_path:
            self._emit("scan.failed", request_id, {"message": "hbatch path is required.", "stderr": "", "exit_code": -1})
            return None
        return hip_path, hbatch_path, scripts_dir, hooks_dir

    def _probe_info_payload(self, request_id: str, payload: dict[str, Any]) -> tuple[str, str, str, Path, Path] | None:
        resolved = self._common_scan_paths_payload(request_id, payload)
        if resolved is None:
            return None
        hip_path, hbatch_path, scripts_dir, hooks_dir = resolved
        rop_path = str(payload.get("rop_path", "") or "").strip()
        if not rop_path:
            self._emit("scan.failed", request_id, {"message": "ROP path is required.", "stderr": "", "exit_code": -1})
            return None
        return hip_path, rop_path, hbatch_path, scripts_dir, hooks_dir

    def _handle_probe_rop_info(self, request_id: str, payload: dict[str, Any]) -> None:
        if self._busy:
            self._emit("scan.failed", request_id, {"message": "Scan worker is busy.", "stderr": "", "exit_code": -1})
            return
        resolved = self._probe_info_payload(request_id, payload)
        if resolved is None:
            return
        self._busy = True
        try:
            hip_path, rop_path, hbatch_path, scripts_dir, hooks_dir = resolved
            probe_script_path = ensure_range_probe_script(
                scripts_dir=scripts_dir,
                hook_script_path_fn=lambda stem: hooks_dir / f"{stem}.py",
            )
            info = probe_rop_info(
                hbatch_path=hbatch_path,
                hip_path=hip_path,
                rop_path=rop_path,
                probe_script_path=probe_script_path,
                hscript_quote=hscript_quote,
            )
            info_payload = None
            if info is not None:
                info_payload = {
                    "error": info.error,
                    "strict_frame_range": info.strict_frame_range,
                    "runtime_start_frame": info.runtime_start_frame,
                    "runtime_end_frame": info.runtime_end_frame,
                    "runtime_step": info.runtime_step,
                    "output_path": info.output_path,
                    "returncode": info.returncode,
                    "combined_output": info.combined_output,
                }
            self._emit("probe.result", request_id, {"hip_path": hip_path, "rop_path": rop_path, "rop_info": info_payload, "error": None})
        except Exception as exc:
            self._emit("scan.failed", request_id, {"message": str(exc), "stderr": traceback.format_exc(), "exit_code": -1})
        finally:
            self._busy = False

    def _handle_probe_strict_range(self, request_id: str, payload: dict[str, Any]) -> None:
        if self._busy:
            self._emit("scan.failed", request_id, {"message": "Scan worker is busy.", "stderr": "", "exit_code": -1})
            return
        resolved = self._probe_info_payload(request_id, payload)
        if resolved is None:
            return
        self._busy = True
        try:
            hip_path, rop_path, hbatch_path, scripts_dir, hooks_dir = resolved
            probe_script_path = ensure_range_probe_script(
                scripts_dir=scripts_dir,
                hook_script_path_fn=lambda stem: hooks_dir / f"{stem}.py",
            )
            info = probe_rop_info(
                hbatch_path=hbatch_path,
                hip_path=hip_path,
                rop_path=rop_path,
                probe_script_path=probe_script_path,
                hscript_quote=hscript_quote,
            )
            strict_value = None if info is None else info.strict_frame_range
            self._emit("probe.strict_range_result", request_id, {"hip_path": hip_path, "rop_path": rop_path, "strict_frame_range": strict_value})
        except Exception as exc:
            self._emit("scan.failed", request_id, {"message": str(exc), "stderr": traceback.format_exc(), "exit_code": -1})
        finally:
            self._busy = False
def main() -> int:
    app = QtCore.QCoreApplication(sys.argv)
    _worker = ScanWorker()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
