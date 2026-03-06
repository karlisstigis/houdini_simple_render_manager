from __future__ import annotations

from typing import Any

from PySide6 import QtCore

from queue_core.queue_tree_ui import refresh_queue_tree_model


class TreeScanCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def defer_refresh_queue_tree_view(self) -> None:
        QtCore.QTimer.singleShot(0, self.refresh_queue_tree_view)

    def refresh_queue_tree_view(self) -> None:
        if not hasattr(self._w, "queue_tree") or self._w.queue_tree is None:
            return
        model = getattr(self._w, "queue_tree_model", None)
        if model is None:
            return
        show_used_only = True
        if hasattr(self._w, "chk_tree_show_used_only") and self._w.chk_tree_show_used_only is not None:
            show_used_only = bool(self._w.chk_tree_show_used_only.isChecked())
        try:
            self._w._suppress_tree_item_changed = True
            refresh_queue_tree_model(
                self._w.queue_tree,
                model,
                self._w.jobs,
                is_locked_job_fn=self._w._is_job_path_sync_locked,
                show_used_only=show_used_only,
                rop_paths_for_hip_fn=(None if show_used_only else self.tree_rop_paths_for_hip),
            )
        finally:
            self._w._suppress_tree_item_changed = False

    @staticmethod
    def sanitize_tree_rop_records(records: Any) -> list[dict[str, Any]]:
        if not isinstance(records, list):
            return []
        sanitized: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            path = str(record.get("path", "") or "").strip()
            if not path:
                continue
            sanitized.append(
                {
                    "path": path,
                    "category": str(record.get("category", "") or "").strip(),
                    "type_name": str(record.get("type_name", "") or "").strip(),
                    "strict_frame_range": None if record.get("strict_frame_range") is None else bool(record.get("strict_frame_range")),
                    "all_frames_single_process": None if record.get("all_frames_single_process") is None else bool(record.get("all_frames_single_process")),
                    "runtime_start_frame": record.get("runtime_start_frame"),
                    "runtime_end_frame": record.get("runtime_end_frame"),
                    "runtime_step": record.get("runtime_step"),
                    "output_path": str(record.get("output_path", "") or ""),
                }
            )
        return sanitized

    def persist_tree_rop_cache(self) -> None:
        self._w._save_queue_state()

    def replace_tree_rop_cache_for_hip(self, hip_path: str, records: list[dict[str, Any]]) -> None:
        hip_value = str(hip_path or "").strip()
        if hip_value:
            self._w._tree_rop_records_by_hip[hip_value] = self.sanitize_tree_rop_records(records)

    def selected_scan_records_for_tree(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        renderable_records = [record for record in records if self._w._is_likely_renderable_scan_node(record)]
        selected_records = renderable_records or records
        return [record for record in selected_records if isinstance(record, dict)]

    def refresh_tree_rop_cache_for_hips(self, hip_paths: list[str]) -> None:
        if not self._w._hbatch_exists():
            return
        changed = False
        for hip_path in sorted({str(path or "").strip() for path in hip_paths if str(path or "").strip()}, key=lambda s: s.lower()):
            records = self._w.scan_coordinator.scan_rop_records_for_hip(hip_path)
            self.replace_tree_rop_cache_for_hip(hip_path, self.selected_scan_records_for_tree(records))
            changed = True
        if changed:
            self.persist_tree_rop_cache()

    def tree_rop_paths_for_hip(self, hip_path: str) -> list[str]:
        hip_value = str(hip_path or "").strip()
        if not hip_value:
            return []
        selected_records = list(self._w._tree_rop_records_by_hip.get(hip_value, []))
        return sorted(
            {
                str(record.get("path", "") or "").strip()
                for record in selected_records
                if str(record.get("path", "") or "").strip()
            },
            key=lambda s: s.lower(),
        )

    def on_tree_show_used_only_toggled(self, checked: bool) -> None:
        self._w.config.set("tree_show_used_only", bool(checked))
        self.refresh_queue_tree_view()

    def handle_scan_worker_message(self, message: dict[str, Any]) -> bool:
        request_id = str(message.get("request_id", "") or "")
        if request_id and self._w._active_scan_request_id and request_id != self._w._active_scan_request_id:
            return True
        payload = dict(message.get("payload", {}) or {})
        message_type = str(message.get("type", "") or "")
        if message_type == "scan.result":
            self._w._active_scan_request_id = ""
            self._w._create_job_scan_in_progress = False
            hip_path = str(payload.get("hip_path", "") or "").strip()
            records = list(payload.get("records", []) or [])
            renderable_records = [r for r in records if self._w._is_likely_renderable_scan_node(r)]
            selected_records = renderable_records or records
            if hip_path:
                self.replace_tree_rop_cache_for_hip(hip_path, selected_records)
                self.persist_tree_rop_cache()
            if selected_records:
                self._w.add_job_panel.set_scanned_rops(selected_records)
                if renderable_records:
                    self._w._set_status_message(
                        f"Scan complete ({len(renderable_records)} likely render nodes, {len(records)} total)",
                        5000,
                    )
                else:
                    self._w._append_log("Stdout", "[Scan] No likely render/output nodes matched; showing all scanned nodes.\n")
                    self._w._set_status_message(f"Scan complete ({len(records)} nodes found, unfiltered)", 5000)
            else:
                self._w.safe_message("Scan", "No nodes found in selected scan targets.")
                self._w._set_status_message("No nodes found in selected scan targets.", 5000)
            self.refresh_queue_tree_view()
            self._w._refresh_ui_state()
            return True
        if message_type == "scan.failed":
            self._w._active_scan_request_id = ""
            self._w._create_job_scan_in_progress = False
            message_text = str(payload.get("message", "") or "Scan failed.")
            details = str(payload.get("stderr", "") or self._w.scan_worker_client.last_stderr_text or "")
            self._w.safe_message("Scan", message_text, details or None)
            self._w._set_status_message(message_text, 5000)
            self._w._refresh_ui_state()
            return True
        return False
