from __future__ import annotations

import traceback
from typing import Any, Callable

from queue_editing import apply_queue_frame_override_text
from queue_models import FrameHandlingMode
from queue_tree_sync import validate_queue_path_value as validate_queue_path_value_model


def apply_queue_cell_edit(
    window: Any,
    row: int,
    col: int,
    text: str,
    *,
    selected_rows_override: list[int] | None = None,
    show_message: Callable[[str, str, str | None], None],
) -> bool:
    try:
        if col not in {0, 1, 2, 3, 4, 5}:
            return False
        if not (0 <= row < len(window.jobs)):
            return False
        job = window.jobs[row]
        if window._is_job_path_sync_locked(job):
            show_message("Please Wait", "Wait for the current path update to finish.", None)
            return False
        inline_target_rows_fn = getattr(window.queue_table, "inline_edit_target_rows", None)
        if selected_rows_override is not None:
            selected_rows = list(selected_rows_override)
        else:
            selected_rows = list(inline_target_rows_fn()) if callable(inline_target_rows_fn) else window._selected_rows()
        if row not in selected_rows:
            selected_rows = [row]
        target_rows = selected_rows
        if any(window._is_job_path_sync_locked(window.jobs[r]) for r in target_rows if 0 <= r < len(window.jobs)):
            show_message("Please Wait", "Wait for the current path update to finish.", None)
            return False
        target_job_ids = [window.jobs[r].id for r in target_rows if 0 <= r < len(window.jobs)]
        preserved_selection_ids = list(target_job_ids)
        before_states = window._job_states_for_ids(target_job_ids)

        def _finish_update(
            after_job_ids: list[str],
            *,
            undo_select_job_ids: list[str] | None = None,
            redo_select_job_ids: list[str] | None = None,
        ) -> bool:
            refresh_ids = list(redo_select_job_ids or preserved_selection_ids or after_job_ids)
            window._push_history_command(
                {
                    "kind": "update_jobs",
                    "before": before_states,
                    "after": window._job_states_for_ids(after_job_ids),
                    "undo_select_job_ids": list(undo_select_job_ids or preserved_selection_ids or target_job_ids),
                    "redo_select_job_ids": refresh_ids,
                }
            )
            window.queue_table_model.refresh_jobs_by_id(after_job_ids)
            window._defer_save_and_refresh_queue(refresh_ids)
            return True

        def _refresh_rejected() -> bool:
            window._refresh_queue_preserve_selection()
            return False

        if col == 5:
            new_mode = FrameHandlingMode.from_label(text)
            changed = False
            for target_row in target_rows:
                target = window.jobs[target_row]
                if window._is_active_job(target):
                    continue
                if target.spec.frame_handling_mode != new_mode:
                    target.spec.frame_handling_mode = new_mode
                    changed = True
            if not changed:
                return True
            return _finish_update(target_job_ids)
        if col == 0:
            new_name = str(text or "").strip()
            changed = False
            for target_row in target_rows:
                target = window.jobs[target_row]
                if window._is_active_job(target):
                    continue
                if target.spec.name != new_name:
                    target.spec.name = new_name
                    changed = True
            if not changed:
                return True
            return _finish_update(target_job_ids)
        if col in {1, 2}:
            try:
                source_text = validate_queue_path_value_model(col, text)
            except ValueError as exc:
                show_message("Invalid Path", str(exc), None)
                return _refresh_rejected()
            old_hip = str(job.spec.hip_path or "").strip()
            old_rop = str(job.spec.rop_path or "").strip()
            if (col == 1 and source_text == old_hip) or (col == 2 and source_text == old_rop):
                return True
            if col == 1:
                affected_before_ids = window._affected_job_ids_for_hip_path_change(old_hip)
            else:
                affected_before_ids = window._affected_job_ids_for_rop_path_change(old_hip, old_rop)
            before_states = window._job_states_for_ids(affected_before_ids)
            if col == 1:
                changed_ids = window._apply_hip_path_change_immediately(old_hip, source_text)
            else:
                changed_ids = window._apply_rop_path_change_immediately(old_hip, old_rop, source_text)
            if not changed_ids:
                return _refresh_rejected()
            selected_ids = preserved_selection_ids or window._selection_ids_for_refresh(changed_ids) or []
            window._defer_finalize_path_change(
                changed_ids=changed_ids,
                before_states=before_states,
                undo_select_job_ids=affected_before_ids,
                redo_select_job_ids=selected_ids,
                status_text="Updating path...",
            )
            return True
        # Frame Range / Step bulk-edit uses the edited row values as the source payload.
        frame_text = str(text or "").strip() if col == 3 else window._queue_model_display_text(row, 3)
        step_text = str(text or "").strip() if col == 4 else window._queue_model_display_text(row, 4)
        changed = False
        for target_row in target_rows:
            target = window.jobs[target_row]
            if window._is_active_job(target):
                continue
            if target.spec.strict_frame_range:
                continue
            try:
                target_frame_text = frame_text if col == 3 else window._queue_edit_frame_text_for_job(target)
                target_step_text = step_text if col == 4 else window._queue_edit_step_text_for_job(target)
                apply_queue_frame_override_text(target, target_frame_text, target_step_text)
                changed = True
            except ValueError as exc:
                show_message("Invalid Frame Override", str(exc), None)
                return _refresh_rejected()
            except Exception as exc:
                show_message("Error", f"Failed to update frame override: {exc}", traceback.format_exc())
                return _refresh_rejected()
        if not changed:
            return _refresh_rejected()
        return _finish_update(target_job_ids)
    except Exception as exc:
        show_message("Queue Edit Error", f"Failed to apply queue edit: {exc}", traceback.format_exc())
        window._refresh_queue_preserve_selection()
        return False
