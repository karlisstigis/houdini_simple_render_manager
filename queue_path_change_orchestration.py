from __future__ import annotations

from typing import Any, Callable

from queue_models import RenderJob
from queue_tree_sync import (
    apply_hip_path_change as apply_hip_path_change_model,
    apply_rop_path_change as apply_rop_path_change_model,
)


def affected_job_ids_for_hip_path_change(jobs: list[RenderJob], old_hip: str) -> list[str]:
    old_key = str(old_hip or "").strip()
    return [job.id for job in jobs if str(job.spec.hip_path or "").strip() == old_key]


def affected_job_ids_for_rop_path_change(jobs: list[RenderJob], hip_path: str, old_rop: str) -> list[str]:
    hip_key = str(hip_path or "").strip()
    old_key = str(old_rop or "").strip()
    return [
        job.id
        for job in jobs
        if str(job.spec.hip_path or "").strip() == hip_key and str(job.spec.rop_path or "").strip() == old_key
    ]


def apply_hip_path_change_immediately(
    jobs: list[RenderJob],
    *,
    old_hip: str,
    new_hip: str,
    running_status: Any,
) -> list[str]:
    changed_jobs = apply_hip_path_change_model(
        jobs,
        old_hip=old_hip,
        new_hip=new_hip,
        running_status=running_status,
    )
    return [job.id for job in changed_jobs]


def apply_rop_path_change_immediately(
    jobs: list[RenderJob],
    *,
    hip_path: str,
    old_rop: str,
    new_rop: str,
    running_status: Any,
) -> list[str]:
    changed_jobs = apply_rop_path_change_model(
        jobs,
        hip_path=hip_path,
        old_rop=old_rop,
        new_rop=new_rop,
        running_status=running_status,
    )
    return [job.id for job in changed_jobs]


def defer_finalize_path_change(
    *,
    changed_ids: list[str],
    before_states: list[dict[str, Any]],
    undo_select_job_ids: list[str],
    redo_select_job_ids: list[str],
    status_text: str,
    begin_path_sync_lock: Callable[[list[str]], None],
    enqueue_path_sync_task: Callable[[dict[str, Any]], None],
) -> list[str]:
    ids = [job_id for job_id in changed_ids if job_id]
    if not ids:
        return []
    begin_path_sync_lock(ids)
    enqueue_path_sync_task(
        {
            "ids": ids,
            "before_states": list(before_states),
            "undo_select_job_ids": list(undo_select_job_ids),
            "redo_select_job_ids": list(redo_select_job_ids),
            "status_text": status_text,
        }
    )
    return ids


def defer_reload_jobs_from_file(
    target_jobs: list[RenderJob],
    *,
    reset_override_to_rop: bool,
    status_text: str,
    notification_label: str,
    job_states_for_ids: Callable[[list[str]], list[dict[str, Any]]],
    begin_path_sync_lock: Callable[[list[str]], None],
    enqueue_path_sync_task: Callable[[dict[str, Any]], None],
) -> list[str]:
    ids = [job.id for job in target_jobs if job is not None and str(job.id or "").strip()]
    if not ids:
        return []
    before_states = job_states_for_ids(ids)
    begin_path_sync_lock(ids)
    enqueue_path_sync_task(
        {
            "ids": ids,
            "before_states": before_states,
            "undo_select_job_ids": ids,
            "redo_select_job_ids": ids,
            "status_text": status_text,
            "reset_override_to_rop": reset_override_to_rop,
            "notification_label": notification_label,
        }
    )
    return ids
