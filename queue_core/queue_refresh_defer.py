from __future__ import annotations

from typing import Any


def should_defer_queue_refresh(
    *,
    focus: Any,
    queue_is_editing: bool,
    focus_in_queue: bool,
    focus_in_add_panel: bool,
    queue_editable_types: tuple[type, ...],
    add_panel_editable_types: tuple[type, ...],
) -> bool:
    if focus is None:
        return False
    if queue_is_editing:
        return True
    if focus_in_queue:
        return isinstance(focus, queue_editable_types)
    if focus_in_add_panel:
        return isinstance(focus, add_panel_editable_types)
    return False


def pending_refresh_args(
    *,
    select_row: int | None = None,
    select_job_id: str | None = None,
    select_job_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "select_row": select_row,
        "select_job_id": select_job_id,
        "select_job_ids": list(select_job_ids or []) or None,
    }


def next_pending_refresh_action(
    pending_args: dict[str, Any] | None,
    *,
    should_defer: bool,
) -> tuple[dict[str, Any] | None, bool]:
    if not pending_args:
        return None, False
    if should_defer:
        return None, True
    return dict(pending_args), False
