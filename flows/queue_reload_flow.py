from __future__ import annotations

from typing import Any, Callable

from queue_core.queue_models import RenderJob


RELOAD_ALL_EMPTY_MESSAGE = "No jobs to reload from file."
RELOAD_ALL_STATUS_TEXT = "Reloading all jobs from file..."
RELOAD_ALL_NOTIFICATION_LABEL = "Reload All"

RELOAD_VALUES_STATUS_TEXT = "Reloading values from file..."
RELOAD_VALUES_NOTIFICATION_LABEL = "Reload Values from File"


def reloadable_jobs(jobs: list[RenderJob], *, running_status: Any) -> list[RenderJob]:
    return [job for job in jobs if job.runtime.status != running_status]


def defer_reload_values_from_file(
    target_jobs: list[RenderJob],
    *,
    defer_reload_jobs_from_file: Callable[..., None],
) -> None:
    defer_reload_jobs_from_file(
        target_jobs,
        reset_override_to_rop=True,
        status_text=RELOAD_VALUES_STATUS_TEXT,
        notification_label=RELOAD_VALUES_NOTIFICATION_LABEL,
    )


def run_reload_all_jobs_from_file(
    jobs: list[RenderJob],
    *,
    running_status: Any,
    write_queue_snapshot: Callable[[str], bool],
    defer_reload_jobs_from_file: Callable[..., None],
    set_status_message: Callable[[str, int], None],
) -> bool:
    target_jobs = reloadable_jobs(jobs, running_status=running_status)
    if not target_jobs:
        set_status_message(RELOAD_ALL_EMPTY_MESSAGE, 3000)
        return False
    write_queue_snapshot("before_reload_all")
    defer_reload_jobs_from_file(
        target_jobs,
        reset_override_to_rop=False,
        status_text=RELOAD_ALL_STATUS_TEXT,
        notification_label=RELOAD_ALL_NOTIFICATION_LABEL,
    )
    return True
