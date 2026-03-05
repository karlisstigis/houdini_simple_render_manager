from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from queue_core.queue_models import JobStatus


@dataclass(frozen=True)
class ActionDecision:
    allowed: bool
    reason: str = ""


def _job_enabled(job: Any | None) -> bool:
    if job is None:
        return False
    spec = getattr(job, "spec", None)
    if spec is not None:
        return bool(spec.enabled)
    return bool(getattr(job, "enabled", False))


def _job_status(job: Any | None) -> JobStatus | None:
    if job is None:
        return None
    runtime = getattr(job, "runtime", None)
    if runtime is not None:
        return runtime.status
    return getattr(job, "status", None)


def _job_strict_frame_range(job: Any | None) -> bool:
    if job is None:
        return False
    spec = getattr(job, "spec", None)
    if spec is not None:
        return bool(spec.strict_frame_range)
    return bool(getattr(job, "strict_frame_range", False))


def is_job_runnable(job: Any | None, *, is_locked: bool = False) -> bool:
    return bool(
        job is not None
        and not is_locked
        and _job_enabled(job)
        and _job_status(job) not in {JobStatus.RUNNING, JobStatus.OFFLINE, JobStatus.DONE}
    )


def can_edit_job(job: Any | None, *, is_active_job: bool, is_locked: bool = False) -> ActionDecision:
    if job is None:
        return ActionDecision(False, "No job selected.")
    if is_locked:
        return ActionDecision(False, "Wait for the current path update to finish.")
    if is_active_job:
        return ActionDecision(False, "Cannot edit the active running job.")
    return ActionDecision(True)


def can_edit_job_column(job: Any | None, *, column: int, is_active_job: bool, is_locked: bool = False) -> ActionDecision:
    base = can_edit_job(job, is_active_job=is_active_job, is_locked=is_locked)
    if not base.allowed:
        return base
    if column in {3, 4} and _job_strict_frame_range(job):
        return ActionDecision(False, "Cannot edit frame range on a strict-range ROP.")
    if column in {0, 1, 2, 3, 4, 5}:
        return ActionDecision(True)
    return ActionDecision(False, "This column is not editable.")


def can_remove_jobs(jobs: list[Any], *, is_active_job_fn, is_locked_job_fn=None) -> ActionDecision:
    if not jobs:
        return ActionDecision(False, "No jobs selected.")
    if is_locked_job_fn is not None and any(is_locked_job_fn(job) for job in jobs):
        return ActionDecision(False, "Wait for the current path update to finish.")
    if any(is_active_job_fn(job) for job in jobs):
        removable = [job for job in jobs if not is_active_job_fn(job)]
        if not removable:
            return ActionDecision(False, "Cannot remove the active running job.")
    return ActionDecision(True)


def can_duplicate_jobs(jobs: list[Any], *, is_active_job_fn, scan_in_progress: bool, is_locked_job_fn=None) -> ActionDecision:
    if scan_in_progress:
        return ActionDecision(False, "Wait for the current scan to finish.")
    if not jobs:
        return ActionDecision(False, "No jobs selected.")
    if is_locked_job_fn is not None and any(is_locked_job_fn(job) for job in jobs):
        return ActionDecision(False, "Wait for the current path update to finish.")
    if all(is_active_job_fn(job) for job in jobs):
        return ActionDecision(False, "Cannot duplicate the active running job.")
    return ActionDecision(True)


def queue_row_status_label(job: Any) -> str:
    if job is None:
        return ""
    status = _job_status(job)
    return "Disabled" if not _job_enabled(job) else str(getattr(status, "value", ""))


def can_open_queue_file(*, queue_active: bool, render_job_active: bool, scan_in_progress: bool) -> ActionDecision:
    if queue_active or render_job_active or scan_in_progress:
        return ActionDecision(False, "Stop the queue before opening another queue file.")
    return ActionDecision(True)


def can_scan_hip(*, scan_in_progress: bool, hbatch_exists: bool) -> ActionDecision:
    if scan_in_progress:
        return ActionDecision(False, "A scan is already running.")
    if not hbatch_exists:
        return ActionDecision(False, "Configure a valid hbatch.exe path first.")
    return ActionDecision(True)


def can_reload_jobs_from_file(*, target_jobs: list[Any], is_active_job_fn, hbatch_exists: bool, is_locked_job_fn=None) -> ActionDecision:
    if not target_jobs:
        return ActionDecision(False, "No jobs selected.")
    if is_locked_job_fn is not None and any(is_locked_job_fn(job) for job in target_jobs):
        return ActionDecision(False, "Wait for the current path update to finish.")
    if any(is_active_job_fn(job) for job in target_jobs):
        return ActionDecision(False, "Cannot reload the active running job from file.")
    if not hbatch_exists:
        return ActionDecision(False, "Configure a valid hbatch.exe path first.")
    return ActionDecision(True)


def can_start_queue(*, queue_active: bool, queue_paused: bool, hbatch_exists: bool, has_runnable: bool, can_start_selected: bool) -> ActionDecision:
    if queue_active:
        if queue_paused:
            return ActionDecision(True)
        return ActionDecision(False, "Queue is already running.")
    if not hbatch_exists:
        return ActionDecision(False, "Configure a valid hbatch.exe path before starting the queue.")
    if not has_runnable and not can_start_selected:
        return ActionDecision(False, "No enabled jobs to run.")
    return ActionDecision(True)


def can_resume_job_from_output(
    job: Any | None,
    *,
    render_job_active: bool,
    queue_active: bool,
    hip_exists: bool,
    hbatch_exists: bool,
) -> ActionDecision:
    if job is None:
        return ActionDecision(False, "No job selected.")
    if render_job_active or queue_active:
        return ActionDecision(False, "Stop the current queue before resuming from output.")
    if _job_status(job) not in {JobStatus.CANCELED, JobStatus.INTERRUPTED, JobStatus.QUEUED, JobStatus.DONE}:
        return ActionDecision(False, "This job cannot be resumed from output.")
    if not hip_exists:
        return ActionDecision(False, "HIP file not found.")
    if not hbatch_exists:
        return ActionDecision(False, "Configure a valid hbatch.exe path before resuming.")
    if not _job_enabled(job):
        return ActionDecision(False, "Job is disabled.")
    if _job_strict_frame_range(job):
        return ActionDecision(False, "Cannot resume from output on a Strict frame range ROP.")
    return ActionDecision(True)


def can_preview_job(*, preview_path_exists: bool, player_path_set: bool, player_exists: bool) -> ActionDecision:
    if not preview_path_exists:
        return ActionDecision(False, "No previewable output path is available for this job.")
    if not player_path_set:
        return ActionDecision(False, "Configure a preview player path in Preferences first.")
    if not player_exists:
        return ActionDecision(False, "Preview player does not exist.")
    return ActionDecision(True)


def can_open_output_folder(*, folder_exists: bool) -> ActionDecision:
    if not folder_exists:
        return ActionDecision(False, "Folder does not exist.")
    return ActionDecision(True)
