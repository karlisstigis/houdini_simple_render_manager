from __future__ import annotations

from typing import Any

from PySide6 import QtCore

from queue_core.queue_refresh_defer import (
    next_pending_refresh_action as next_pending_refresh_action_model,
    pending_refresh_args as pending_refresh_args_model,
)
from queue_core.queue_refresh_selection import (
    clamped_select_row as clamped_select_row_model,
    preserved_selection as preserved_selection_model,
    target_selection as target_selection_model,
)
from queue_core.queue_targeting import job_row_by_id as job_row_by_id_model
from queue_core.queue_targeting import selection_ids_for_refresh as selection_ids_for_refresh_model


QUEUE_REFRESH_DEFER_MS = 200
QUEUE_ROW_REFRESH_DEFER_MS = 50
QUEUE_ROW_REFRESH_RETRY_MS = 100


class QueueRefreshCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def flush_pending_queue_refresh(self) -> None:
        args, should_reschedule = next_pending_refresh_action_model(
            self._w._pending_queue_refresh_args,
            should_defer=self._w._queue_refresh_should_defer(),
        )
        if should_reschedule:
            self._w._pending_queue_refresh_timer.start(QUEUE_REFRESH_DEFER_MS)
            return
        if args is None:
            return
        self._w._pending_queue_refresh_args = None
        self.refresh_queue_table(**args)

    def save_and_refresh_queue(
        self,
        *,
        select_job_id: str | None = None,
        select_job_ids: list[str] | None = None,
        select_row: int | None = None,
    ) -> None:
        self._w._save_queue_state()
        self.refresh_queue_table(
            select_row=select_row,
            select_job_id=select_job_id,
            select_job_ids=select_job_ids,
        )

    def selection_ids_for_refresh(self, fallback_job_ids: list[str] | None = None) -> list[str] | None:
        return selection_ids_for_refresh_model(self._w._selected_job_ids(), fallback_job_ids)

    def defer_save_and_refresh_queue(
        self,
        select_job_ids: list[str] | None = None,
        *,
        block_interaction: bool = False,
        status_text: str | None = None,
    ) -> None:
        ids = list(select_job_ids or [])
        if block_interaction:
            self._w._begin_interaction_lock(status_text or "Applying change...")

        def _finish(selection_ids: list[str]) -> None:
            try:
                self.save_and_refresh_queue(
                    select_job_ids=self.selection_ids_for_refresh(selection_ids)
                )
            finally:
                if block_interaction:
                    self._w._end_interaction_lock()

        QtCore.QTimer.singleShot(0, lambda selection_ids=ids: _finish(selection_ids))

    def refresh_queue_table(
        self,
        select_row: int | None = None,
        select_job_id: str | None = None,
        select_job_ids: list[str] | None = None,
    ) -> None:
        if self._w._queue_refresh_should_defer():
            self._w._pending_queue_refresh_args = pending_refresh_args_model(
                select_row=select_row,
                select_job_id=select_job_id,
                select_job_ids=select_job_ids,
            )
            self._w._pending_queue_refresh_timer.start(QUEUE_REFRESH_DEFER_MS)
            return
        if self._w._pending_job_row_refresh_timer.isActive():
            self._w._pending_job_row_refresh_timer.stop()
        self._w._pending_job_row_refresh_ids.clear()
        current_selected = self._w._selected_job()
        preserved_job_ids, preserved_job_id = preserved_selection_model(
            select_row=select_row,
            select_job_id=select_job_id,
            select_job_ids=select_job_ids,
            current_selected_job_ids=[j.id for j in self._w._selected_jobs()],
            current_selected_job_id=(current_selected.id if current_selected is not None else None),
        )

        selection_model = self._w.queue_table.selectionModel()
        selection_blocker = QtCore.QSignalBlocker(selection_model) if selection_model is not None else None
        try:
            self._w.queue_table.setUpdatesEnabled(False)
            current_job_ids = [job.id for job in self._w.jobs]
            can_soft_refresh = (
                current_job_ids == self._w._last_queue_job_id_order
                and self._w.queue_table_model.rowCount() == len(current_job_ids)
            )
            if can_soft_refresh:
                self._w.queue_table_model.refresh_all_rows_data()
            else:
                self._w.queue_table_model.refresh_all()
                self._w._last_queue_job_id_order = list(current_job_ids)

            target_job_id, target_job_ids, target_job_id_set = target_selection_model(
                select_job_id=select_job_id,
                select_job_ids=select_job_ids,
                preserved_job_id=preserved_job_id,
                preserved_job_ids=preserved_job_ids,
            )
            should_reapply_selection = not (
                can_soft_refresh
                and select_row is None
                and select_job_id is None
                and not select_job_ids
            )
            if should_reapply_selection:
                self._w.queue_table.clearSelection()
                selected_applied = False
                if target_job_ids:
                    sm = self._w.queue_table.selectionModel()
                    if sm is not None:
                        for row, job in enumerate(self._w.jobs):
                            if job.id in target_job_id_set:
                                model_idx = self._w._queue_view_index_from_source_row(row, 0)
                                if not model_idx.isValid():
                                    continue
                                sm.select(
                                    model_idx,
                                    QtCore.QItemSelectionModel.SelectionFlag.Select
                                    | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                                )
                                selected_applied = True
                elif target_job_id:
                    for row, job in enumerate(self._w.jobs):
                        if job.id == target_job_id:
                            model_idx = self._w._queue_view_index_from_source_row(row, 0)
                            if model_idx.isValid():
                                self._w.queue_table.selectRow(model_idx.row())
                                selected_applied = True
                            break
                elif select_row is not None and self._w.jobs:
                    clamped_row = clamped_select_row_model(select_row, job_count=len(self._w.jobs))
                    if clamped_row is None:
                        clamped_row = 0
                    model_idx = self._w._queue_view_index_from_source_row(clamped_row, 0)
                    if model_idx.isValid():
                        self._w.queue_table.selectRow(model_idx.row())
                        selected_applied = True

                if selected_applied:
                    sm = self._w.queue_table.selectionModel()
                    row = self._w._selected_row()
                    if sm is not None and row >= 0:
                        idx = self._w._queue_view_index_from_source_row(row, 0)
                        sm.setCurrentIndex(idx, QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)
        finally:
            self._w.queue_table.setUpdatesEnabled(True)
            if selection_blocker is not None:
                del selection_blocker

        self._w._refresh_queue_tree_view()
        self.sync_last_job_status_snapshot()
        self._w._update_job_properties_panel()
        self._w._refresh_ui_state()

    def refresh_job_row(self, job_id: str) -> None:
        target_id = str(job_id or "").strip()
        if not target_id:
            return
        row = job_row_by_id_model(self._w.jobs, target_id)
        if row < 0:
            return
        if self._w._queue_refresh_should_defer():
            self._w._pending_queue_refresh_args = pending_refresh_args_model(select_job_id=target_id)
            self._w._pending_queue_refresh_timer.start(QUEUE_REFRESH_DEFER_MS)
            return
        job = self._w.jobs[row]
        current_status = job.runtime.status
        previous_status = self._w._last_job_status_by_id.get(target_id)
        self._w._last_job_status_by_id[target_id] = current_status
        status_changed = previous_status is not None and previous_status != current_status
        if status_changed:
            self._w.queue_table_model.refresh_job_by_id(target_id)
            if target_id in set(self._w._selected_job_ids()):
                self._w._update_job_properties_panel()
            self._w._refresh_ui_state()
            return
        self._w._pending_job_row_refresh_ids.add(target_id)
        self._w._pending_job_row_refresh_timer.start(QUEUE_ROW_REFRESH_DEFER_MS)

    def flush_pending_job_row_refreshes(self) -> None:
        if not self._w._pending_job_row_refresh_ids:
            return
        if self._w._queue_refresh_should_defer():
            self._w._pending_job_row_refresh_timer.start(QUEUE_ROW_REFRESH_RETRY_MS)
            return
        pending_ids = list(self._w._pending_job_row_refresh_ids)
        self._w._pending_job_row_refresh_ids.clear()
        selected_ids = set(self._w._selected_job_ids())
        refresh_ids = [job_id for job_id in pending_ids if job_id in {job.id for job in self._w.jobs}]
        if not refresh_ids:
            return
        self._w.queue_table_model.refresh_jobs_by_id(refresh_ids)
        if selected_ids and any(job_id in selected_ids for job_id in refresh_ids):
            self._w._update_job_properties_panel()
        self._w._refresh_ui_state()

    def sync_last_job_status_snapshot(self) -> None:
        current_ids = {job.id for job in self._w.jobs}
        for job in self._w.jobs:
            self._w._last_job_status_by_id[job.id] = job.runtime.status
        stale_ids = [job_id for job_id in self._w._last_job_status_by_id if job_id not in current_ids]
        for job_id in stale_ids:
            self._w._last_job_status_by_id.pop(job_id, None)
