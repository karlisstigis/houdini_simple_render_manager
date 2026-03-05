from __future__ import annotations

from typing import Callable


def start_queue_runnable_state(*, selected_job, is_runnable: Callable[[object], bool], jobs: list[object]) -> tuple[bool, bool]:
    can_start_selected = bool(is_runnable(selected_job))
    has_runnable = any(bool(is_runnable(job)) for job in jobs)
    return can_start_selected, has_runnable


def blocked_start_title(reason: str) -> str:
    text = str(reason or "")
    return "hbatch Missing" if "hbatch" in text.lower() else "Queue Empty"


def should_set_selected_rerun_status(selected_job) -> bool:
    if selected_job is None:
        return False
    status = getattr(getattr(selected_job, "runtime", None), "status", None)
    # Imported lazily to avoid circular import here.
    from queue_core.queue_models import JobStatus

    return status not in {JobStatus.RUNNING, JobStatus.QUEUED}
