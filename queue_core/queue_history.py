from __future__ import annotations

from typing import Any, Callable


def should_push_history_command(*, history_applying: bool, command: dict[str, Any]) -> bool:
    if history_applying:
        return False
    kind = str(command.get("kind", "") or "")
    if kind in {"insert_jobs", "remove_jobs"} and not list(command.get("entries", []) or []):
        return False
    if kind == "update_jobs" and list(command.get("before", []) or []) == list(command.get("after", []) or []):
        return False
    if kind == "reorder_jobs" and list(command.get("before_order", []) or []) == list(command.get("after_order", []) or []):
        return False
    return True


def bounded_undo_stack(undo_stack: list[dict[str, Any]], *, max_size: int) -> list[dict[str, Any]]:
    if len(undo_stack) <= max_size:
        return undo_stack
    return list(undo_stack[-max_size:])


def history_command_candidate_ids(command: dict[str, Any]) -> set[str]:
    candidate_ids: set[str] = set()
    for key in ("before", "after"):
        for state in list(command.get(key, []) or []):
            if not isinstance(state, dict):
                continue
            job_id = str(state.get("id", "") or "").strip()
            if job_id:
                candidate_ids.add(job_id)
    for entry in list(command.get("entries", []) or []):
        if not isinstance(entry, dict):
            continue
        job_state = entry.get("job")
        if not isinstance(job_state, dict):
            continue
        job_id = str(job_state.get("id", "") or "").strip()
        if job_id:
            candidate_ids.add(job_id)
    for key in ("before_order", "after_order", "undo_select_job_ids", "redo_select_job_ids"):
        for job_id in list(command.get(key, []) or []):
            text = str(job_id or "").strip()
            if text:
                candidate_ids.add(text)
    return candidate_ids


def history_command_targets_job(command: dict[str, Any], *, active_job_id: str) -> bool:
    if not active_job_id:
        return False
    return active_job_id in history_command_candidate_ids(command)


def apply_history_command(
    command: dict[str, Any],
    *,
    undo: bool,
    remove_jobs_by_ids: Callable[[list[str]], None],
    insert_jobs_from_entries: Callable[[list[dict[str, Any]]], None],
    apply_job_states: Callable[[list[dict[str, Any]]], None],
    apply_job_order: Callable[[list[str]], None],
) -> list[str]:
    kind = str(command.get("kind", "") or "")
    undo_select = list(command.get("undo_select_job_ids", []) or [])
    redo_select = list(command.get("redo_select_job_ids", []) or [])
    select_ids = undo_select if undo else redo_select
    if kind == "insert_jobs":
        entries = list(command.get("entries", []) or [])
        if undo:
            remove_jobs_by_ids([str(entry.get("job", {}).get("id", "") or "") for entry in entries])
        else:
            insert_jobs_from_entries(entries)
    elif kind == "remove_jobs":
        entries = list(command.get("entries", []) or [])
        if undo:
            insert_jobs_from_entries(entries)
        else:
            remove_jobs_by_ids([str(entry.get("job", {}).get("id", "") or "") for entry in entries])
    elif kind == "update_jobs":
        states = list(command.get("before", []) if undo else command.get("after", []))
        apply_job_states(states)
    elif kind == "reorder_jobs":
        order = list(command.get("before_order", []) if undo else command.get("after_order", []))
        apply_job_order(order)
    return select_ids
