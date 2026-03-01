from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from queue_models import JobStatus


@dataclass(frozen=True)
class ActionDecision:
    allowed: bool
    reason: str = ""


def is_job_runnable(job: Any | None) -> bool:
    return bool(
        job is not None
        and getattr(job, "enabled", False)
        and getattr(job, "status", None) not in {JobStatus.RUNNING, JobStatus.OFFLINE, JobStatus.DONE}
    )


def can_edit_job(job: Any | None, *, is_active_job: bool) -> ActionDecision:
    if job is None:
        return ActionDecision(False, "No job selected.")
    if is_active_job:
        return ActionDecision(False, "Cannot edit the active running job.")
    return ActionDecision(True)


def can_edit_job_column(job: Any | None, *, column: int, is_active_job: bool) -> ActionDecision:
    base = can_edit_job(job, is_active_job=is_active_job)
    if not base.allowed:
        return base
    if column in {3, 4} and bool(getattr(job, "strict_frame_range", False)):
        return ActionDecision(False, "Cannot edit frame range on a strict-range ROP.")
    if column in {0, 1, 2, 3, 4, 5}:
        return ActionDecision(True)
    return ActionDecision(False, "This column is not editable.")


def can_remove_jobs(jobs: list[Any], *, is_active_job_fn) -> ActionDecision:
    if not jobs:
        return ActionDecision(False, "No jobs selected.")
    if any(is_active_job_fn(job) for job in jobs):
        removable = [job for job in jobs if not is_active_job_fn(job)]
        if not removable:
            return ActionDecision(False, "Cannot remove the active running job.")
    return ActionDecision(True)


def can_duplicate_jobs(jobs: list[Any], *, is_active_job_fn, scan_in_progress: bool) -> ActionDecision:
    if scan_in_progress:
        return ActionDecision(False, "Wait for the current scan to finish.")
    if not jobs:
        return ActionDecision(False, "No jobs selected.")
    if all(is_active_job_fn(job) for job in jobs):
        return ActionDecision(False, "Cannot duplicate the active running job.")
    return ActionDecision(True)


def queue_row_status_label(job: Any) -> str:
    if job is None:
        return ""
    return "Disabled" if not getattr(job, "enabled", False) else str(getattr(getattr(job, "status", None), "value", ""))


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
