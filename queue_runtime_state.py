"""Pure queue runtime/state display helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from queue_models import RenderJob


def job_time_remaining_display(job: RenderJob, terminal_statuses: set[object]) -> str:
    if job.runtime.status in terminal_statuses:
        return ""
    return str(job.view.est_job_time_text or "-")


def job_end_time_display(job: RenderJob) -> str:
    if job.runtime.finished_at is None:
        return ""
    return job.runtime.finished_at.strftime("%H:%M:%S")


def job_started_time_display(job: RenderJob) -> str:
    if job.runtime.started_at is None:
        return ""
    return job.runtime.started_at.strftime("%H:%M:%S")


def job_total_time_display(job: RenderJob, *, now_fn: Callable[[], datetime]) -> str:
    if job.runtime.started_at is None:
        return ""
    end_ts = job.runtime.finished_at or now_fn()
    try:
        seconds = max(0.0, (end_ts - job.runtime.started_at).total_seconds())
    except Exception:
        return ""
    return format_duration_short(seconds)


def job_frame_display(job: RenderJob, terminal_statuses: set[object]) -> str:
    if job.runtime.status in terminal_statuses:
        return ""
    return str(job.view.progress_text or "")


def reset_job_process_attempt_state(job: RenderJob, *, preserve_output: bool = True) -> None:
    job.view.phase_text = "Starting"
    job.view.progress_text = "-"
    job.view.percent_text = "-"
    job.view.usd_build_percent = None
    job.view.last_frame_seen = None
    job.view.build_pass_completed = False
    job.runtime.allframesatonce_enabled = None
    job.view.prev_frame_time_text = "-"
    job.view.avg_frame_time_text = "-"
    job.view.est_job_time_text = "-"
    job.view.render_frame_started_at.clear()
    job.view.render_frame_durations_sec.clear()
    job.view.render_completed_frames.clear()
    if not preserve_output:
        job.view.out_path = ""
        job.view.out_file_sample_path = ""


def initialize_job_chunk_runtime(
    job: RenderJob,
    *,
    forced_ranges: list[tuple[int, int, int]] | None,
    retry_count_value: int,
    resolve_job_range_for_execution: Callable[[RenderJob], tuple[int, int, int] | None],
    expand_ranges_with_chunking: Callable[[list[tuple[int, int, int]]], list[tuple[int, int, int]]],
) -> None:
    job.runtime.chunk_ranges_runtime.clear()
    job.runtime.chunk_start_frame_runtime = None
    job.runtime.chunk_end_frame_runtime = None
    job.runtime.chunk_step_runtime = None
    job.runtime.chunk_index_runtime = 0
    job.runtime.chunk_total_runtime = 0
    job.runtime.chunk_attempt_runtime = 0
    job.runtime.chunk_retry_count_runtime = retry_count_value
    job.runtime.chunk_retry_total_failures_runtime = 0

    if forced_ranges is not None:
        chunks = expand_ranges_with_chunking(forced_ranges)
    else:
        resolved = resolve_job_range_for_execution(job)
        if resolved is None:
            return
        base_start, base_end, base_step = resolved
        if base_step <= 0 or base_end < base_start:
            return
        chunks = expand_ranges_with_chunking([(base_start, base_end, base_step)])
    if not chunks:
        return
    job.runtime.chunk_ranges_runtime = chunks
    job.runtime.chunk_total_runtime = len(chunks)
    job.runtime.chunk_index_runtime = 0
    job.runtime.chunk_attempt_runtime = 1
    cs, ce, cstep = chunks[0]
    job.runtime.chunk_start_frame_runtime = int(cs)
    job.runtime.chunk_end_frame_runtime = int(ce)
    job.runtime.chunk_step_runtime = int(cstep)


def total_frames_for_job(job: RenderJob) -> int | None:
    if job.spec.frame_range_mode == "override":
        return job.total_override_frames()
    if (
        job.runtime.runtime_start_frame is not None
        and job.runtime.runtime_end_frame is not None
        and job.runtime.runtime_step not in (None, 0)
    ):
        try:
            return int(((job.runtime.runtime_end_frame - job.runtime.runtime_start_frame) / job.runtime.runtime_step) + 1)
        except Exception:
            return None
    return None


def update_job_render_timing_stats(
    job: RenderJob,
    *,
    format_duration_short_fn: Callable[[float], str],
) -> None:
    durations = [d for d in job.view.render_frame_durations_sec if d >= 0]
    if durations:
        job.view.prev_frame_time_text = format_duration_short_fn(durations[-1])
        avg = sum(durations) / len(durations)
        job.view.avg_frame_time_text = format_duration_short_fn(avg)
        total = total_frames_for_job(job)
        if total and total > 0:
            remaining = max(0, total - (int(job.runtime.resume_completed_baseline_count or 0) + len(durations)))
            job.view.est_job_time_text = format_duration_short_fn(avg * remaining)
        else:
            job.view.est_job_time_text = "-"
    else:
        job.view.prev_frame_time_text = "-"
        job.view.avg_frame_time_text = "-"
        job.view.est_job_time_text = "-"


def format_duration_short(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"
