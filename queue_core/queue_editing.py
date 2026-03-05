"""Queue row edit / job mutation helpers (path edits, frame override parsing, status resets)."""

from __future__ import annotations

import re

from queue_core.queue_models import JobStatus, RenderJob


def clear_job_resume_runtime_state(job: RenderJob) -> None:
    job.runtime.resume_start_frame_runtime = None
    job.runtime.resume_end_frame_runtime = None
    job.runtime.resume_step_runtime = None
    job.runtime.resume_completed_baseline_count = 0


def reset_job_state(job: RenderJob) -> None:
    if job.runtime.status == JobStatus.RUNNING:
        return
    job.runtime.status = JobStatus.QUEUED
    job.runtime.offline_previous_status = None
    job.runtime.started_at = None
    job.runtime.finished_at = None
    job.runtime.exit_code = None
    job.runtime.error_summary = ""
    job.runtime.interrupted_reason = ""
    job.runtime.offline_detected_reason = ""
    job.view.phase_text = ""
    job.view.progress_text = "-"
    job.view.percent_text = "-"
    job.view.usd_build_percent = None
    job.view.last_frame_seen = None
    job.view.build_pass_completed = False
    job.view.prev_frame_time_text = "-"
    job.view.avg_frame_time_text = "-"
    job.view.est_job_time_text = "-"
    job.view.render_frame_started_at.clear()
    job.view.render_frame_durations_sec.clear()
    job.view.render_completed_frames.clear()
    clear_job_resume_runtime_state(job)


def mark_job_offline(job: RenderJob, reason: str | None = None) -> None:
    if job.runtime.status != JobStatus.OFFLINE:
        job.runtime.offline_previous_status = job.runtime.status
        job.runtime.status = JobStatus.OFFLINE
    if reason:
        job.runtime.error_summary = reason


def restore_job_online_status(job: RenderJob) -> None:
    if job.runtime.status != JobStatus.OFFLINE:
        return
    restore = job.runtime.offline_previous_status or JobStatus.QUEUED
    if restore == JobStatus.RUNNING:
        restore = JobStatus.QUEUED
    job.runtime.status = restore
    job.runtime.offline_previous_status = None


def apply_queue_path_text(job: RenderJob, column: int, new_text: str) -> None:
    text = (new_text or "").strip()
    if column == 1:
        if not text:
            raise ValueError("HIP path cannot be empty.")
        job.spec.hip_path = text
        return
    if column == 2:
        if not text:
            raise ValueError("ROP path cannot be empty.")
        if not text.startswith("/"):
            raise ValueError("ROP path should look like /out/my_rop.")
        job.spec.rop_path = text
        return


def apply_queue_frame_override_text(job: RenderJob, frame_text: str, step_text: str) -> None:
    frame_text = (frame_text or "").strip()
    step_text = (step_text or "").strip()

    if frame_text.lower() in {"", "from rop"}:
        job.spec.frame_range_mode = "use_rop"
        job.spec.start_frame = None
        job.spec.end_frame = None
        job.spec.step = None
        return

    m_range = re.fullmatch(r"\s*(-?\d+)\s*-\s*(-?\d+)\s*", frame_text)
    m_single = re.fullmatch(r"\s*(-?\d+)\s*", frame_text)
    if m_range:
        start_frame = int(m_range.group(1))
        end_frame = int(m_range.group(2))
    elif m_single:
        start_frame = int(m_single.group(1))
        end_frame = start_frame
    else:
        raise ValueError(
            "Frame Range must be 'Start-End' (for example: 250-800), a single frame number (for example: 250), or 'From ROP'."
        )
    if end_frame < start_frame:
        raise ValueError("End frame must be >= start frame.")

    if step_text.lower() in {"", "from rop"}:
        step_val = 1
    else:
        try:
            step_val = int(step_text)
        except ValueError as exc:
            raise ValueError("Step must be a positive integer.") from exc
        if step_val <= 0:
            raise ValueError("Step must be a positive integer.")

    job.spec.frame_range_mode = "override"
    job.spec.start_frame = start_frame
    job.spec.end_frame = end_frame
    job.spec.step = step_val
    # Preserve cached ROP-derived runtime range/step so UI can compare per-cell overrides
    # against the original ROP values without re-probing the HIP file on every edit.
