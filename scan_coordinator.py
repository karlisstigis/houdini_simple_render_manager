from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from action_policy import can_scan_hip
from queue_models import RenderJob
from rop_metadata import RopInfo, apply_rop_info_to_job as apply_rop_info_to_job_model, rop_info_from_scan_record as rop_info_from_scan_record_model


@dataclass
class ScanCoordinatorHooks:
    current_hbatch_path: Callable[[], str]
    project_houdini_scripts_dir: Callable[[], Path]
    hooks_dir_path: Callable[[], Path]
    hbatch_exists: Callable[[], bool]
    scan_in_progress: Callable[[], bool]
    send_scan_request: Callable[[str, dict[str, Any]], bool]
    request_scan_sync: Callable[[str, dict[str, Any], int], dict[str, Any] | None]
    append_log: Callable[[str, str], None]
    safe_message: Callable[[str, str, str | None], None]
    set_status_message: Callable[[str, int | None], None]
    normalize_output_display_path: Callable[[str], str]
    set_scan_hip_path_requested: Callable[[str], None]


class ScanCoordinator:
    def __init__(self, hooks: ScanCoordinatorHooks) -> None:
        self._hooks = hooks

    def build_request_payload(self, *, hip_path: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "hip_path": str(hip_path or "").strip(),
            "hbatch_path": self._hooks.current_hbatch_path(),
            "scripts_dir": str(self._hooks.project_houdini_scripts_dir()),
            "hooks_dir": str(self._hooks.hooks_dir_path()),
        }
        if extra:
            payload.update(extra)
        return payload

    def request_sync_payload(
        self,
        message_type: str,
        *,
        hip_path: str,
        timeout_ms: int = 30000,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        return self._hooks.request_scan_sync(
            message_type,
            self.build_request_payload(hip_path=hip_path, extra=extra),
            timeout_ms,
        )

    def probe_rop_info(self, hip_path: str, rop_path: str) -> RopInfo | None:
        if not self._hooks.hbatch_exists():
            return None
        response = self.request_sync_payload(
            "scan.rop_info",
            hip_path=hip_path,
            timeout_ms=25000,
            extra={"rop_path": rop_path},
        )
        if response is None:
            self._hooks.append_log("Stderr", f"[ROP Probe] Failed for {rop_path}\n")
            return None
        if str(response.get("type", "") or "") == "scan.failed":
            payload = dict(response.get("payload", {}) or {})
            self._hooks.append_log("Stderr", f"[ROP Probe] {payload.get('message', 'Probe failed')}\n")
            return None
        payload = dict(response.get("payload", {}) or {})
        info_payload = payload.get("rop_info")
        if not isinstance(info_payload, dict):
            self._hooks.append_log("Stderr", f"[ROP Probe] Failed for {rop_path}\n")
            return None
        return RopInfo(
            error=info_payload.get("error"),
            strict_frame_range=info_payload.get("strict_frame_range"),
            runtime_start_frame=info_payload.get("runtime_start_frame"),
            runtime_end_frame=info_payload.get("runtime_end_frame"),
            runtime_step=info_payload.get("runtime_step"),
            output_path=str(info_payload.get("output_path", "") or ""),
            returncode=info_payload.get("returncode"),
            combined_output=str(info_payload.get("combined_output", "") or ""),
        )

    def probe_and_apply_job_rop_metadata(self, job: RenderJob) -> str | None:
        try:
            info = self.probe_rop_info(job.spec.hip_path, job.spec.rop_path)
            if info is not None:
                apply_rop_info_to_job_model(
                    job,
                    info,
                    self._hooks.normalize_output_display_path,
                    apply_runtime_range=True,
                )
            if info is None:
                return None
            err = str(info.error) if info.runtime_start_frame is None or info.runtime_end_frame is None else None
            if err:
                self._hooks.append_log("Stderr", f"[ROP Probe] {job.spec.rop_path}: {err}\n")
                return err
            if info.returncode not in (None, 0):
                self._hooks.append_log("Stderr", f"[ROP Probe] hbatch exited {info.returncode} while resolving {job.spec.rop_path}\n")
            return None
        except Exception as exc:
            self._hooks.append_log("Stderr", f"[ROP Probe] Unexpected error for {job.spec.rop_path}: {exc}\n")
            return f"probe_failed: {exc}"

    def probe_rop_strict_frame_range(self, hip_path: str, rop_path: str) -> bool | None:
        response = self.request_sync_payload(
            "scan.strict_range",
            hip_path=hip_path,
            timeout_ms=25000,
            extra={"rop_path": rop_path},
        )
        if response is None or str(response.get("type", "") or "") == "scan.failed":
            return None
        payload = dict(response.get("payload", {}) or {})
        value = payload.get("strict_frame_range")
        return None if value is None else bool(value)

    def scan_rop_info_for_hip(self, hip_path: str) -> dict[str, RopInfo]:
        if not self._hooks.hbatch_exists():
            return {}
        response = self.request_sync_payload(
            "scan.nodes",
            hip_path=hip_path,
            timeout_ms=30000,
            extra={"roots": ["/out", "/stage"]},
        )
        if response is None or str(response.get("type", "") or "") != "scan.result":
            return {}
        records = list(dict(response.get("payload", {}) or {}).get("records", []) or [])
        info_map: dict[str, RopInfo] = {}
        for record in records:
            if not isinstance(record, dict):
                continue
            path = str(record.get("path", "") or "").strip()
            if path:
                info_map[path] = rop_info_from_scan_record_model(record)
        return info_map

    def handle_scan_requested(self, request: dict) -> bool:
        hip_path = str(request.get("hip_path", "")).strip()
        scan_out = bool(request.get("scan_out", True))
        scan_stage = bool(request.get("scan_stage", True))
        roots: list[str] = []
        if scan_out:
            roots.append("/out")
        if scan_stage:
            roots.append("/stage")

        decision = can_scan_hip(
            scan_in_progress=self._hooks.scan_in_progress(),
            hbatch_exists=self._hooks.hbatch_exists(),
        )
        if not decision.allowed:
            title = "Scan In Progress" if self._hooks.scan_in_progress() else "hbatch Missing"
            self._hooks.safe_message(title, decision.reason, None)
            return False
        if not hip_path:
            self._hooks.safe_message("HIP Required", "Select a .hip file before scanning.", None)
            return False
        if not Path(hip_path).exists():
            self._hooks.safe_message("HIP Missing", f"HIP file does not exist:\n{hip_path}", None)
            return False
        if not roots:
            self._hooks.safe_message("Scan Targets", "Enable at least one scan target: /out and/or /stage.", None)
            return False

        self._hooks.set_scan_hip_path_requested(hip_path)
        roots_label = " + ".join(roots)
        self._hooks.append_log("Stdout", f"\n=== Scan {roots_label}: {hip_path} ===\n")
        self._hooks.set_status_message(f"Scanning {roots_label} ...", None)

        started = self._hooks.send_scan_request(
            "scan.nodes",
            self.build_request_payload(hip_path=hip_path, extra={"roots": roots}),
        )
        if not started:
            self._hooks.safe_message("Scan Failed", "Failed to start hbatch for scanning.", None)
            self._hooks.set_status_message("Scan failed to start", 5000)
        return started
