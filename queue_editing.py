"""Queue row edit / job mutation helpers (path edits, frame override parsing, status resets)."""

from __future__ import annotations

import re
from typing import Any


def clear_job_resume_runtime_state(job: Any) -> None:
    job.resume_start_frame_runtime = None
    job.resume_end_frame_runtime = None
    job.resume_step_runtime = None
    job.resume_completed_baseline_count = 0


def reset_job_state(job: Any) -> None:
    try:
        status_type = type(job.status)
        running = status_type.RUNNING
        queued = status_type.QUEUED
    except Exception:
        running = None
        queued = None
    if running is not None and job.status == running:
        return
    if queued is not None:
        job.status = queued
    job.offline_previous_status = None
    job.started_at = None
    job.finished_at = None
    job.exit_code = None
    job.error_summary = ""
    job.interrupted_reason = ""
    job.offline_detected_reason = ""
    job.phase_text = ""
    job.progress_text = "-"
    job.percent_text = "-"
    job.usd_build_percent = None
    job.last_frame_seen = None
    job.build_pass_completed = False
    job.prev_frame_time_text = "-"
    job.avg_frame_time_text = "-"
    job.est_job_time_text = "-"
    job.render_frame_started_at.clear()
    job.render_frame_durations_sec.clear()
    job.render_completed_frames.clear()
    clear_job_resume_runtime_state(job)


def mark_job_offline(job: Any, reason: str | None = None) -> None:
    try:
        offline_enum = type(job.status).OFFLINE
    except Exception:
        offline_enum = None
    if offline_enum is not None and job.status != offline_enum:
        job.offline_previous_status = job.status
        job.status = offline_enum
    if reason:
        job.error_summary = reason


def restore_job_online_status(job: Any) -> None:
    try:
        status_type = type(job.status)
        offline_enum = status_type.OFFLINE
        running_enum = status_type.RUNNING
        queued_enum = status_type.QUEUED
    except Exception:
        return
    if job.status != offline_enum:
        return
    restore = job.offline_previous_status or queued_enum
    if restore == running_enum:
        restore = queued_enum
    job.status = restore
    job.offline_previous_status = None


def apply_queue_path_text(job: Any, column: int, new_text: str) -> None:
    text = (new_text or "").strip()
    if column == 1:
        if not text:
            raise ValueError("HIP path cannot be empty.")
        job.hip_path = text
        return
    if column == 2:
        if not text:
            raise ValueError("ROP path cannot be empty.")
        if not text.startswith("/"):
            raise ValueError("ROP path should look like /out/my_rop.")
        job.rop_path = text
        return


def apply_queue_frame_override_text(job: Any, frame_text: str, step_text: str) -> None:
    frame_text = (frame_text or "").strip()
    step_text = (step_text or "").strip()

    if frame_text.lower() in {"", "from rop"}:
        job.frame_range_mode = "use_rop"
        job.start_frame = None
        job.end_frame = None
        job.step = None
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

    job.frame_range_mode = "override"
    job.start_frame = start_frame
    job.end_frame = end_frame
    job.step = step_val
    # Preserve cached ROP-derived runtime range/step so UI can compare per-cell overrides
    # against the original ROP values without re-probing the HIP file on every edit.
