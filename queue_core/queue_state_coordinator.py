from __future__ import annotations

from pathlib import Path
from typing import Any

from queue_core.queue_models import JobStatus, RenderJob
from app_core.recovery_reporting import build_startup_recovery_summary
from queue_core.queue_persistence import load_queue_payload, save_queue_payload
from flows.queue_state_io import (
    load_queue_state as load_queue_state_model,
    save_queue_state as save_queue_state_model,
)


class QueueStateCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def queue_view_to_persisted_dict(self) -> dict[str, Any]:
        payload = self._w.queue_view_state_payload()
        payload["tree_rop_records_by_hip"] = {
            hip_path: self._w._sanitize_tree_rop_records(records)
            for hip_path, records in self._w._tree_rop_records_by_hip.items()
            if str(hip_path or "").strip()
        }
        return payload

    def load_queue_from_path(self, path: Path) -> bool:
        try:
            loaded_jobs, queue_view, _active_job_id = load_queue_state_model(
                path,
                load_queue_payload_fn=load_queue_payload,
                job_from_persisted_dict_fn=lambda item, active_id: self._w._job_from_persisted_dict(
                    item,
                    active_job_id=active_id,
                ),
            )
            self._w.jobs = loaded_jobs
            self._w._set_current_queue_file_path(path)
            self._restore_tree_rop_cache(queue_view)
            self._w._reset_queue_view_to_defaults()
            self._w._apply_queue_view_from_persisted_data(queue_view)
            self._w._clear_history()
            self._apply_load_refresh_and_recovery()
            return True
        except Exception as exc:
            self._w._append_log("Stderr", f"[Queue] Failed to load queue: {exc}\n")
            return False

    def save_queue_state(self, path: Path | None = None) -> bool:
        try:
            target_path = save_queue_state_model(
                current_queue_path=self._w._current_queue_file_path(),
                path_override=path,
                jobs=self._w.jobs,
                queue_view=self.queue_view_to_persisted_dict(),
                active_job_id=self._w.current_job_id,
                save_queue_payload_fn=save_queue_payload,
            )
            self._w._set_current_queue_file_path(target_path)
            return True
        except (OSError, TypeError, ValueError) as exc:
            self._w._append_log("Stderr", f"[Queue] Failed to save queue: {exc}\n")
            return False

    def load_persisted_queue(self) -> None:
        path = self._w._current_queue_file_path()
        if path.exists():
            self.load_queue_from_path(path)

    def _restore_tree_rop_cache(self, queue_view: dict[str, Any]) -> None:
        self._w._tree_rop_records_by_hip = {}
        raw_tree_cache = queue_view.get("tree_rop_records_by_hip", {})
        if not isinstance(raw_tree_cache, dict):
            return
        for hip_path, records in raw_tree_cache.items():
            hip_value = str(hip_path or "").strip()
            if not hip_value:
                continue
            self._w._tree_rop_records_by_hip[hip_value] = self._w._sanitize_tree_rop_records(records)

    def _apply_load_refresh_and_recovery(self) -> None:
        self.apply_startup_job_file_checks()
        recovery_summary = build_startup_recovery_summary(self._w.jobs)
        if self._w.jobs:
            self._w._refresh_queue_table(select_row=0)
        else:
            self._w._refresh_queue_table()
        if recovery_summary is None:
            self._w._last_recovery_headline = ""
            self.schedule_startup_reload_all()
            return
        self._w._last_recovery_headline = recovery_summary.headline
        self._w._append_notification_message(recovery_summary.headline, "warning")
        self._w._append_log("Stderr", f"[Recovery] {recovery_summary.headline}\n")
        for notice in recovery_summary.notices:
            self._w._append_notification_message(notice.message, notice.severity)
            self._w._append_log("Stderr", f"[Recovery] {notice.technical_message}\n")
        self._w._set_status_message(recovery_summary.headline, 6000)
        self.schedule_startup_reload_all()

    def apply_startup_job_file_checks(self) -> bool:
        if not self._w._startup_check_files_on_open():
            return False
        changed = False
        for job in self._w.jobs:
            if self._apply_startup_file_check_to_job(job):
                changed = True
        return changed

    def schedule_startup_reload_all(self) -> bool:
        if not self._w._startup_reload_all_jobs_on_open():
            return False
        if not self._w.jobs:
            return False
        preserved_selection_job_ids = list(self._w._selected_job_ids())
        self._w._schedule_deferred(
            lambda ids=preserved_selection_job_ids: self._w._reload_all_jobs_from_files(
                preserved_selection_job_ids=ids
            ),
            0,
        )
        return True

    def _apply_startup_file_check_to_job(self, job: RenderJob) -> bool:
        if job.runtime.status == JobStatus.RUNNING:
            return False
        hip_path = str(job.spec.hip_path or "").strip()
        hip_exists = bool(hip_path) and Path(hip_path).exists()
        if not hip_exists:
            before_status = job.runtime.status
            before_error = str(job.runtime.error_summary or "")
            self._w._mark_job_offline(job, "HIP file not found.")
            return before_status != job.runtime.status or before_error != str(job.runtime.error_summary or "")
        if job.runtime.status == JobStatus.OFFLINE and str(job.runtime.error_summary or "").strip() == "HIP file not found.":
            self._w._restore_job_online_status(job)
            job.runtime.error_summary = ""
            job.runtime.offline_detected_reason = ""
            return True
        return False
