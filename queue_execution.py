"""Queue execution policy helpers.

Pure orchestration helpers for frame-handling planning, chunk transitions,
next-job selection, and render-finish state evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from queue_models import RenderJob


@dataclass
class FrameHandlingPlan:
    effective_mode: Any
    forced_ranges: list[tuple[int, int, int]] | None = None
    baseline_done: int = 0
    already_complete: bool = False
    info_message: str = ""


@dataclass
class RenderFinishEvaluation:
    logical_success: bool
    was_canceled: bool
    was_offline: bool


def select_next_runnable_job(
    jobs: list[RenderJob],
    *,
    start_index: int,
    is_runnable: Callable[[Any], bool],
    started_job_ids: set[str],
) -> RenderJob | None:
    start_idx = max(0, min(int(start_index), len(jobs)))
    for job in jobs[start_idx:]:
        if is_runnable(job) and getattr(job, "id", None) not in started_job_ids:
            return job
    return None


def plan_frame_handling(
    job: RenderJob,
    *,
    overwrite_mode: Any,
    render_missing_mode: Any,
    render_from_first_missing_mode: Any,
    compute_resume_from_output: Callable[[RenderJob], tuple[int, int, int, int] | None],
    compute_missing_ranges_from_output: Callable[[RenderJob], tuple[list[tuple[int, int, int]], int] | None],
) -> FrameHandlingPlan:
    effective_mode = job.spec.frame_handling_mode
    if effective_mode != overwrite_mode and job.spec.strict_frame_range:
        return FrameHandlingPlan(
            effective_mode=overwrite_mode,
            info_message=f"[Frame Handling] {job.display_name()}: strict range ROP, falling back to Overwrite.\n",
        )

    if effective_mode == render_from_first_missing_mode:
        auto_resume = compute_resume_from_output(job)
        if auto_resume is None:
            return FrameHandlingPlan(
                effective_mode=overwrite_mode,
                info_message=f"[Render From First Missing] {job.display_name()}: output scan unavailable, using Overwrite.\n",
            )
        resume_start, resume_end, resume_step, baseline_done = auto_resume
        if resume_start > resume_end:
            return FrameHandlingPlan(
                effective_mode=effective_mode,
                baseline_done=int(baseline_done),
                already_complete=True,
                info_message=(
                    f"[Render From First Missing] {job.display_name()}: all frames already exist ({baseline_done}). "
                    "Marking Done.\n"
                ),
            )
        return FrameHandlingPlan(
            effective_mode=effective_mode,
            forced_ranges=[(int(resume_start), int(resume_end), int(resume_step))],
            baseline_done=int(baseline_done),
            info_message=(
                f"[Render From First Missing] {job.display_name()}: start={resume_start}, "
                f"end={resume_end}, step={resume_step}, existing={baseline_done}\n"
            ),
        )

    if effective_mode == render_missing_mode:
        missing_plan = compute_missing_ranges_from_output(job)
        if missing_plan is None:
            return FrameHandlingPlan(
                effective_mode=overwrite_mode,
                info_message=f"[Render Missing] {job.display_name()}: output scan unavailable, using Overwrite.\n",
            )
        missing_ranges, baseline_done = missing_plan
        if not missing_ranges:
            return FrameHandlingPlan(
                effective_mode=effective_mode,
                baseline_done=int(baseline_done),
                already_complete=True,
                info_message=f"[Render Missing] {job.display_name()}: all frames already exist ({baseline_done}). Marking Done.\n",
            )
        missing_count = sum(((e - s) // st) + 1 for s, e, st in missing_ranges if st > 0 and e >= s)
        return FrameHandlingPlan(
            effective_mode=effective_mode,
            forced_ranges=list(missing_ranges),
            baseline_done=int(baseline_done),
            info_message=(
                f"[Render Missing] {job.display_name()}: missing={missing_count}, "
                f"existing={baseline_done}, runs={len(missing_ranges)}\n"
            ),
        )

    return FrameHandlingPlan(effective_mode=effective_mode)


def mark_job_done_without_render(job: RenderJob, *, done_status: Any, now_fn: Callable[[], datetime]) -> None:
    job.runtime.status = done_status
    job.runtime.finished_at = now_fn()
    job.runtime.exit_code = 0
    job.view.phase_text = ""
    job.view.progress_text = "Done"
    job.view.percent_text = "100%"


def advance_job_to_next_chunk(job: RenderJob) -> bool:
    if not job.runtime.chunk_ranges_runtime:
        return False
    if job.runtime.chunk_index_runtime + 1 >= len(job.runtime.chunk_ranges_runtime):
        return False
    chunk_start, chunk_end, chunk_step = job.runtime.chunk_ranges_runtime[job.runtime.chunk_index_runtime]
    try:
        done_count = ((int(chunk_end) - int(chunk_start)) // int(chunk_step)) + 1
    except (TypeError, ValueError, ZeroDivisionError):
        done_count = 0
    job.runtime.resume_completed_baseline_count = int(max(0, job.runtime.resume_completed_baseline_count) + max(0, done_count))
    job.runtime.chunk_index_runtime += 1
    job.runtime.chunk_attempt_runtime = 1
    cs, ce, cstep = job.runtime.chunk_ranges_runtime[job.runtime.chunk_index_runtime]
    job.runtime.chunk_start_frame_runtime = int(cs)
    job.runtime.chunk_end_frame_runtime = int(ce)
    job.runtime.chunk_step_runtime = int(cstep)
    return True


def retry_current_chunk(job: RenderJob) -> bool:
    if not job.runtime.chunk_ranges_runtime:
        return False
    max_retries = int(max(0, job.runtime.chunk_retry_count_runtime))
    if job.runtime.chunk_attempt_runtime >= (1 + max_retries):
        return False
    job.runtime.chunk_attempt_runtime += 1
    job.runtime.chunk_retry_total_failures_runtime += 1
    return True


def apply_render_finished_state(
    job: RenderJob,
    *,
    exit_code: int,
    exit_status: Any,
    done_status: Any,
    failed_status: Any,
    canceled_status: Any,
    now_fn: Callable[[], datetime],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    was_canceled: bool,
) -> RenderFinishEvaluation:
    job.runtime.finished_at = now_fn()
    job.runtime.exit_code = int(exit_code)
    error_lower = (job.runtime.error_summary or "").lower()
    explicit_render_failure = any(
        token in error_lower for token in ("render failed", "failed to complete render", "command exit code")
    )
    was_offline = bool(job.runtime.offline_detected_reason)
    normal_exit = getattr(type(exit_status), "NormalExit", None)
    process_ok = bool(exit_status == normal_exit and exit_code == 0)
    logical_success = bool(process_ok and not explicit_render_failure and not was_offline and not was_canceled)

    if was_offline:
        mark_job_offline(job, job.runtime.offline_detected_reason or None)
        job.view.phase_text = ""
        job.view.progress_text = "Offline"
        job.view.percent_text = "-"
    elif was_canceled:
        job.runtime.status = canceled_status
        job.view.phase_text = ""
        job.view.progress_text = "Canceled"
        job.view.percent_text = "-"
        if not job.runtime.error_summary:
            job.runtime.error_summary = "Canceled by user."
    elif explicit_render_failure:
        job.runtime.status = failed_status
        job.view.phase_text = ""
        if not job.view.progress_text:
            job.view.progress_text = "Failed"
        if not job.view.percent_text:
            job.view.percent_text = "-"
    elif process_ok:
        job.runtime.status = done_status
        job.view.phase_text = ""
        job.view.progress_text = "Done"
        job.view.percent_text = "100%" if job.spec.frame_range_mode == "override" else "-"
    else:
        job.runtime.status = failed_status
        job.view.phase_text = ""
        if not job.runtime.error_summary:
            job.runtime.error_summary = f"Process exited with code {exit_code} ({exit_status.name})."
        if not job.view.progress_text:
            job.view.progress_text = "Failed"
        if not job.view.percent_text:
            job.view.percent_text = "-"

    return RenderFinishEvaluation(
        logical_success=logical_success,
        was_canceled=was_canceled,
        was_offline=was_offline,
    )
