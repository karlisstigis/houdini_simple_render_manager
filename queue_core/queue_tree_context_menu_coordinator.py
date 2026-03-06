from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtWidgets

from app_core.action_policy import can_reload_jobs_from_file
from queue_core.queue_tree_ui import TREE_HIP_ROLE, TREE_KIND_ROLE, TREE_ROP_ROLE


class QueueTreeContextMenuCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def show_queue_tree_context_menu(self, pos: QtCore.QPoint) -> None:
        if not hasattr(self._w, "queue_tree") or self._w.queue_tree is None:
            return
        index = self._w.queue_tree.indexAt(pos)
        if not index.isValid():
            return
        self._w.queue_tree.setCurrentIndex(index)
        kind = str(index.data(TREE_KIND_ROLE) or "").strip().lower()
        hip_path = str(index.data(TREE_HIP_ROLE) or "").strip()
        rop_path = str(index.data(TREE_ROP_ROLE) or "").strip()
        target_jobs = self._w._tree_context_target_jobs(index)
        hip_jobs = [job for job in self._w.jobs if str(job.spec.hip_path or "").strip() == hip_path] if hip_path else []
        can_create_job = bool(kind == "rop" and rop_path)
        if not target_jobs and not hip_jobs and not can_create_job:
            return

        menu = QtWidgets.QMenu(self._w)
        act_select = menu.addAction("Select")
        act_select.setEnabled(bool(target_jobs))
        act_create_job = menu.addAction("Create Job")
        act_create_job.setEnabled(can_create_job)
        reload_target_jobs = self._w._tree_context_reload_target_jobs(index, hip_jobs or target_jobs)
        reload_decision = can_reload_jobs_from_file(
            target_jobs=reload_target_jobs,
            is_active_job_fn=self._w._is_active_job,
            hbatch_exists=self._w._hbatch_exists(),
            is_locked_job_fn=self._w._is_job_path_sync_locked,
        )
        act_reload = menu.addAction("Reload File")
        act_reload.setEnabled(bool(reload_decision.allowed))
        menu.addSeparator()
        act_remove = menu.addAction("Remove")
        any_locked = any(self._w._is_job_path_sync_locked(job) for job in target_jobs)
        act_remove.setEnabled((not any_locked) and any(not self._w._is_active_job(job) for job in target_jobs))

        chosen = menu.exec(self._w.queue_tree.viewport().mapToGlobal(pos))
        if chosen is None:
            return
        if chosen == act_select:
            if target_jobs:
                self._w._refresh_queue_table(select_job_ids=[job.id for job in target_jobs])
            return
        if chosen == act_create_job:
            if can_create_job:
                self._w._create_job_from_tree_rop(hip_path=hip_path, rop_path=rop_path)
            return
        if chosen == act_reload:
            if not reload_decision.allowed:
                self._w.safe_message("Reload File", reload_decision.reason)
                return
            self._w._defer_reload_jobs_from_file(
                reload_target_jobs,
                reset_override_to_rop=False,
                status_text="Reloading file from disk...",
                notification_label="Reload File",
            )
            return
        if chosen == act_remove:
            if any_locked:
                self._w.safe_message("Please Wait", "Wait for the current path update to finish.")
                return
            removable = [job for job in target_jobs if not self._w._is_active_job(job)]
            if not removable:
                self._w.safe_message("Cannot Remove", "Cannot remove the active running job.")
                return
            removed_entries = [
                {"index": idx, "job": self._w._job_to_persisted_dict(job)}
                for idx, job in enumerate(self._w.jobs)
                if job.id in {target.id for target in removable}
            ]
            removable_ids = {job.id for job in removable}
            running_blocked = any(self._w._is_active_job(job) for job in target_jobs)
            self._w.jobs = [job for job in self._w.jobs if job.id not in removable_ids]
            self._w._push_history_command(
                {
                    "kind": "remove_jobs",
                    "entries": removed_entries,
                    "undo_select_job_ids": [entry["job"]["id"] for entry in removed_entries],
                    "redo_select_job_ids": [],
                }
            )
            self._w._save_and_refresh_queue()
            if running_blocked:
                self._w.safe_message(
                    "Some Jobs Not Removed",
                    "The active running job cannot be removed. Other matching jobs were removed.",
                )
