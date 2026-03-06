from __future__ import annotations

from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets

from app_core.action_policy import (
    can_duplicate_jobs,
    can_open_output_folder,
    can_preview_job,
    can_reload_jobs_from_file,
)
from flows.queue_context_menu_flow import (
    apply_job_mutation_with_history as apply_job_mutation_with_history_model,
    build_queue_context_menu_availability as build_queue_context_menu_availability_model,
    queue_context_action_key as queue_context_action_key_model,
)
from flows.queue_reload_flow import defer_reload_values_from_file as defer_reload_values_from_file_model
from queue_core.queue_models import JobStatus


class QueueContextMenuCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def show_queue_context_menu(self, pos: QtCore.QPoint) -> None:
        idx = self._w.queue_table.indexAt(pos)
        if idx.isValid():
            sm = self._w.queue_table.selectionModel()
            if sm is not None and not sm.isRowSelected(idx.row(), QtCore.QModelIndex()):
                self._w.queue_table.selectRow(idx.row())
        job = self._w._selected_job()
        if job is None:
            return
        selected_jobs = self._w._selected_jobs()
        target_jobs = selected_jobs or [job]
        any_active = any(j.status == JobStatus.RUNNING and self._w.current_job_id == j.id for j in target_jobs)

        menu = QtWidgets.QMenu(self._w)
        out_folder = self._w._output_folder_from_value(job.view.out_path)
        has_finished_jobs = any(j.runtime.status in {JobStatus.DONE, JobStatus.FAILED} for j in self._w.jobs)
        any_locked = any(self._w._is_job_path_sync_locked(j) for j in target_jobs)
        reload_decision = can_reload_jobs_from_file(
            target_jobs=target_jobs,
            is_active_job_fn=self._w._is_active_job,
            hbatch_exists=self._w._hbatch_exists(),
            is_locked_job_fn=self._w._is_job_path_sync_locked,
        )
        duplicate_decision = can_duplicate_jobs(
            target_jobs,
            is_active_job_fn=self._w._is_active_job,
            scan_in_progress=self._w._scan_in_progress(),
            is_locked_job_fn=self._w._is_job_path_sync_locked,
        )
        preview_path = self._w._job_preview_path(job)
        preview_player_path = self._w._current_player_path()
        preview_decision = can_preview_job(
            preview_path_exists=bool(preview_path),
            player_path_set=bool(preview_player_path),
            player_exists=bool(preview_player_path and Path(preview_player_path).exists()),
        )
        open_folder_decision = can_open_output_folder(folder_exists=bool(out_folder and out_folder.exists()))
        reset_value_allowed = bool(
            idx.isValid() and idx.column() in {3, 4} and any(self._w._job_can_reset_cached_cell(t, idx.column()) for t in target_jobs)
        )
        availability = build_queue_context_menu_availability_model(
            job_enabled=bool(job.spec.enabled),
            any_active=bool(any_active),
            any_locked=bool(any_locked),
            has_finished_jobs=bool(has_finished_jobs),
            reset_value_allowed=bool(reset_value_allowed),
            reload_allowed=bool(reload_decision.allowed),
            duplicate_allowed=bool(duplicate_decision.allowed),
            preview_allowed=bool(preview_decision.allowed),
            open_folder_allowed=bool(open_folder_decision.allowed),
        )

        act_toggle = menu.addAction(availability.toggle_text)
        act_toggle.setEnabled(availability.toggle_enabled)
        act_reset = menu.addAction("Reset State")
        act_reset.setEnabled(availability.reset_enabled)
        act_reset_cell_cached = None
        if idx.isValid() and idx.column() in {3, 4}:
            menu.addSeparator()
            act_reset_cell_cached = menu.addAction("Reset Value")
            act_reset_cell_cached.setEnabled(availability.reset_value_enabled)
        act_reload_from_rop = menu.addAction("Reload Values from File")
        act_reload_from_rop.setEnabled(availability.reload_enabled)
        menu.addSeparator()
        act_duplicate = menu.addAction("Duplicate")
        act_duplicate.setEnabled(availability.duplicate_enabled)
        act_remove = menu.addAction("Remove")
        act_remove.setEnabled(availability.remove_enabled)
        act_clear_finished = menu.addAction("Clear Finished")
        act_clear_finished.setEnabled(availability.clear_finished_enabled)
        menu.addSeparator()
        act_preview = menu.addAction("Preview")
        act_preview.setEnabled(availability.preview_enabled)
        act_open_folder = menu.addAction("Open Folder")
        act_open_folder.setEnabled(availability.open_folder_enabled)

        chosen = menu.exec(self._w.queue_table.viewport().mapToGlobal(pos))
        if chosen is None:
            return
        action_key = queue_context_action_key_model(
            chosen,
            {
                "preview": act_preview,
                "open_folder": act_open_folder,
                "toggle": act_toggle,
                "reset": act_reset,
                "reset_value": act_reset_cell_cached,
                "reload_from_rop": act_reload_from_rop,
                "duplicate": act_duplicate,
                "remove": act_remove,
                "clear_finished": act_clear_finished,
            },
        )
        if action_key == "preview":
            self._w._preview_job(job)
            return
        if action_key == "open_folder" and out_folder is not None:
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(out_folder)))
            return
        if action_key == "toggle":
            new_enabled = not job.spec.enabled
            apply_job_mutation_with_history_model(
                target_jobs,
                is_active_job=self._w._is_active_job,
                mutate_job=lambda target: setattr(target, "enabled", new_enabled),
                job_states_for_ids=self._w._job_states_for_ids,
                push_history_command=self._w._push_history_command,
                save_and_refresh_queue=lambda ids: self._w._save_and_refresh_queue(select_job_ids=ids),
            )
            return
        if action_key == "reset":
            apply_job_mutation_with_history_model(
                target_jobs,
                is_active_job=self._w._is_active_job,
                mutate_job=self._w._reset_job_state,
                job_states_for_ids=self._w._job_states_for_ids,
                push_history_command=self._w._push_history_command,
                save_and_refresh_queue=lambda ids: self._w._save_and_refresh_queue(select_job_ids=ids),
            )
            return
        if action_key == "reset_value":
            target_ids = [j.id for j in target_jobs]
            before_states = self._w._job_states_for_ids(target_ids)
            if idx.isValid() and idx.column() in {3, 4} and self._w._reset_cached_cell_for_jobs(idx.column(), target_jobs):
                after_states = self._w._job_states_for_ids(target_ids)
                self._w._push_history_command(
                    {
                        "kind": "update_jobs",
                        "before": before_states,
                        "after": after_states,
                        "undo_select_job_ids": target_ids,
                        "redo_select_job_ids": target_ids,
                    }
                )
                self._w._save_and_refresh_queue(select_job_ids=[j.id for j in target_jobs])
            return
        if action_key == "reload_from_rop":
            if not reload_decision.allowed:
                self._w.safe_message("Reload From File", reload_decision.reason)
                return
            defer_reload_values_from_file_model(
                target_jobs,
                defer_reload_jobs_from_file=self._w._defer_reload_jobs_from_file,
            )
            return
        if action_key == "duplicate":
            self._w._duplicate_selected_jobs()
            return
        if action_key == "remove":
            self._w._remove_selected_job()
            return
        if action_key == "clear_finished":
            self._w._clear_finished_jobs()
