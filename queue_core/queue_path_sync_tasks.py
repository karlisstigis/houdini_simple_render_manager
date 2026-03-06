from __future__ import annotations

from typing import Any, Callable

from queue_core.queue_models import RenderJob


def enqueue_path_sync_task(pending_tasks: list[dict[str, Any]], task: dict[str, Any]) -> None:
    pending_tasks.append(dict(task))


def should_schedule_next_path_sync_task(*, path_sync_task_active: bool, pending_tasks: list[dict[str, Any]]) -> bool:
    return not path_sync_task_active and bool(pending_tasks)


def run_next_path_sync_task(
    *,
    jobs: list[RenderJob],
    pending_tasks: list[dict[str, Any]],
    offline_status: Any,
    refresh_queue_tree_view: Callable[[], None],
    refresh_jobs_from_rop_metadata: Callable[[list[RenderJob], bool], list[str]],
    end_path_sync_lock: Callable[[list[str]], None],
    push_history_command: Callable[[dict[str, Any]], None],
    job_states_for_ids: Callable[[list[str]], list[dict[str, Any]]],
    save_queue_state: Callable[[], bool],
    append_notification_message: Callable[[str, str], None],
) -> bool:
    if not pending_tasks:
        return False
    task = pending_tasks.pop(0)
    ids = list(task.get("ids", []) or [])
    before_states = list(task.get("before_states", []) or [])
    undo_select_job_ids = list(task.get("undo_select_job_ids", []) or [])
    redo_select_job_ids = list(task.get("redo_select_job_ids", []) or [])
    reset_override_to_rop = bool(task.get("reset_override_to_rop", False))
    notification_label = str(task.get("notification_label", "") or "").strip()
    processed_ids: set[str] = set()
    refresh_needed = False
    try:
        ids_set = set(ids)
        target_jobs = [job for job in jobs if job.id in ids_set]
        by_hip: dict[str, list[RenderJob]] = {}
        for job in target_jobs:
            by_hip.setdefault(str(job.spec.hip_path or ""), []).append(job)
        for hip_jobs in by_hip.values():
            refresh_jobs_from_rop_metadata(hip_jobs, reset_override_to_rop)
            hip_job_ids = [job.id for job in hip_jobs]
            processed_ids.update(hip_job_ids)
            end_path_sync_lock(hip_job_ids)
        push_history_command(
            {
                "kind": "update_jobs",
                "before": before_states,
                "after": job_states_for_ids(ids),
                "undo_select_job_ids": undo_select_job_ids,
                "redo_select_job_ids": redo_select_job_ids,
            }
        )
        save_queue_state()
        if notification_label:
            affected_jobs = [job for job in jobs if job.id in ids_set]
            offline_count = sum(1 for job in affected_jobs if job.runtime.status == offline_status)
            message = f"{notification_label}: {len(affected_jobs)} job(s) refreshed"
            if offline_count:
                message += f", {offline_count} offline"
            append_notification_message(message + ".", "warning" if offline_count else "info")
        refresh_queue_tree_view()
        refresh_needed = True
    finally:
        remaining_ids = [job_id for job_id in ids if job_id not in processed_ids]
        if remaining_ids:
            end_path_sync_lock(remaining_ids)
    return refresh_needed
