from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from queue_core.queue_models import RenderJob


@dataclass(frozen=True)
class QueueContextMenuAvailability:
    toggle_text: str
    toggle_enabled: bool
    reset_enabled: bool
    reset_value_enabled: bool
    reload_enabled: bool
    duplicate_enabled: bool
    remove_enabled: bool
    clear_finished_enabled: bool
    preview_enabled: bool
    open_folder_enabled: bool


def build_queue_context_menu_availability(
    *,
    job_enabled: bool,
    any_active: bool,
    any_locked: bool,
    has_finished_jobs: bool,
    reset_value_allowed: bool,
    reload_allowed: bool,
    duplicate_allowed: bool,
    preview_allowed: bool,
    open_folder_allowed: bool,
) -> QueueContextMenuAvailability:
    disabled_by_activity_or_lock = bool(any_active or any_locked)
    return QueueContextMenuAvailability(
        toggle_text="Disable" if job_enabled else "Enable",
        toggle_enabled=not disabled_by_activity_or_lock,
        reset_enabled=not disabled_by_activity_or_lock,
        reset_value_enabled=bool((not any_locked) and reset_value_allowed),
        reload_enabled=bool(reload_allowed),
        duplicate_enabled=bool(duplicate_allowed),
        remove_enabled=not disabled_by_activity_or_lock,
        clear_finished_enabled=bool(has_finished_jobs),
        preview_enabled=bool(preview_allowed),
        open_folder_enabled=bool(open_folder_allowed),
    )


def queue_context_action_key(chosen: Any, action_map: dict[str, Any]) -> str | None:
    for key, action in action_map.items():
        if chosen == action:
            return key
    return None


def apply_job_mutation_with_history(
    target_jobs: list[RenderJob],
    *,
    is_active_job: Callable[[RenderJob], bool],
    mutate_job: Callable[[RenderJob], None],
    job_states_for_ids: Callable[[list[str]], list[dict[str, Any]]],
    push_history_command: Callable[[dict[str, Any]], None],
    save_and_refresh_queue: Callable[[list[str]], None],
) -> bool:
    target_ids = [job.id for job in target_jobs if str(job.id or "").strip()]
    if not target_ids:
        return False
    before_states = job_states_for_ids(target_ids)
    for target in target_jobs:
        if not is_active_job(target):
            mutate_job(target)
    after_states = job_states_for_ids(target_ids)
    push_history_command(
        {
            "kind": "update_jobs",
            "before": before_states,
            "after": after_states,
            "undo_select_job_ids": target_ids,
            "redo_select_job_ids": target_ids,
        }
    )
    save_and_refresh_queue(target_ids)
    return True
