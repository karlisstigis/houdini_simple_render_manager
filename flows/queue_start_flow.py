from __future__ import annotations

from dataclasses import dataclass


def start_queue_mode(
    *,
    queue_active: bool,
    queue_paused: bool,
    resume_existing: bool,
    allowed: bool,
) -> str:
    if queue_active:
        if queue_paused and resume_existing:
            return "resume_existing"
        return "already_active"
    if not allowed:
        return "blocked"
    return "start_new"


@dataclass(frozen=True)
class JobStartPreflight:
    allowed: bool
    abort_queue: bool
    dialog_title: str | None
    dialog_message: str | None
    offline_reason: str | None


def evaluate_job_start_preflight(*, hbatch_exists: bool, hip_exists: bool) -> JobStartPreflight:
    if not hbatch_exists:
        return JobStartPreflight(
            allowed=False,
            abort_queue=True,
            dialog_title="hbatch Missing",
            dialog_message="Configured hbatch.exe no longer exists.",
            offline_reason=None,
        )
    if not hip_exists:
        return JobStartPreflight(
            allowed=False,
            abort_queue=False,
            dialog_title=None,
            dialog_message=None,
            offline_reason="HIP file not found.",
        )
    return JobStartPreflight(
        allowed=True,
        abort_queue=False,
        dialog_title=None,
        dialog_message=None,
        offline_reason=None,
    )
