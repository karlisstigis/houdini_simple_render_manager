from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6 import QtCore

from queue_execution import (
    mark_job_done_without_render as mark_job_done_without_render_model,
    plan_frame_handling as plan_frame_handling_model,
)
from queue_models import FrameHandlingMode, JobStatus, RenderJob


def _schedule_job_continuation(window: Any, job: RenderJob, delay_ms: int) -> None:
    window._reset_job_process_attempt_state(job)
    if delay_ms > 0:
        QtCore.QTimer.singleShot(delay_ms, lambda j=job: window._start_job_process_continuation(j))
        return
    window._start_job_process_continuation(job)


def start_job_runtime(window: Any, job: RenderJob) -> None:
    window._clear_job_resume_runtime_state(job)
    frame_plan = plan_frame_handling_model(
        job,
        overwrite_mode=FrameHandlingMode.OVERWRITE,
        render_missing_mode=FrameHandlingMode.RENDER_MISSING,
        render_from_first_missing_mode=FrameHandlingMode.RENDER_FROM_FIRST_MISSING,
        compute_resume_from_output=lambda target: window._compute_resume_from_output(target, interactive=False),
        compute_missing_ranges_from_output=lambda target: window._compute_missing_ranges_from_output(target, interactive=False),
    )
    baseline_done = int(frame_plan.baseline_done)
    forced_ranges = frame_plan.forced_ranges
    if frame_plan.info_message:
        window._append_log("Stdout", frame_plan.info_message)
    if frame_plan.already_complete:
        mark_job_done_without_render_model(job, done_status=JobStatus.DONE, now_fn=datetime.now)
        window._save_queue_state()
        window._refresh_queue_table(select_job_id=job.id)
        window._maybe_start_next_job()
        return

    # Build runtime chunk plan after frame-handling planning.
    window._initialize_job_chunk_runtime(job, forced_ranges=forced_ranges)
    if not job.runtime.chunk_ranges_runtime and window._chunking_enabled():
        # Fall back to non-chunked execution if range resolution failed.
        job.runtime.chunk_retry_count_runtime = window._retry_count_value()
    build_range = window._current_retained_usd_build_range(job)
    if build_range is not None:
        job.runtime.retained_usd_build_start_frame = int(build_range[0])
        job.runtime.retained_usd_build_end_frame = int(build_range[1])
        job.runtime.retained_usd_build_step = int(build_range[2])
    else:
        job.runtime.retained_usd_build_start_frame = None
        job.runtime.retained_usd_build_end_frame = None
        job.runtime.retained_usd_build_step = None

    resume_baseline = int(max(0, baseline_done))

    job.runtime.status = JobStatus.RUNNING
    job.runtime.started_at = datetime.now()
    job.runtime.finished_at = None
    job.runtime.exit_code = None
    job.runtime.error_summary = ""
    job.runtime.offline_detected_reason = ""
    window._reset_job_process_attempt_state(job)
    job.runtime.runtime_start_frame = None
    job.runtime.runtime_end_frame = None
    job.runtime.runtime_step = None
    # Keep the last known output path visible until a newer one is detected.
    job.runtime.resume_completed_baseline_count = resume_baseline
    window._cancel_phase_promote()
    window.current_job_id = job.id
    window.canceling_current_job = False
    window._jobs_started_this_run.add(job.id)
    try:
        window._queue_next_search_index = window.jobs.index(job) + 1
    except ValueError:
        window._queue_next_search_index = 0

    try:
        Path(job.runtime.log_file_path).parent.mkdir(parents=True, exist_ok=True)
        window.current_job_log_handle = open(job.runtime.log_file_path, "a", encoding="utf-8", errors="replace")
    except OSError as exc:
        window.current_job_log_handle = None
        window._append_log("Stderr", f"[Log] Failed to open log file: {exc}\n")

    window._write_job_log(f"=== Job Started: {job.display_name()} ===\n")
    window._write_job_log(f"HIP: {job.spec.hip_path}\nROP: {job.spec.rop_path}\nFrames: {job.frame_display()}\n")
    if job.runtime.chunk_total_runtime > 1:
        window._write_job_log(
            f"[Chunking] {job.runtime.chunk_total_runtime} chunks | retries={job.runtime.chunk_retry_count_runtime} | delay={window._retry_delay_value()}s\n"
        )
    if resume_baseline > 0:
        window._write_job_log(f"[Frame Handling] Existing frames baseline: {resume_baseline}\n")

    window._append_log("Stdout", f"\n=== Render Start: {job.display_name()} ===\n")
    if job.runtime.chunk_total_runtime > 1:
        window._append_log(
            "Stdout",
            f"[Chunk] {job.runtime.chunk_index_runtime + 1}/{job.runtime.chunk_total_runtime} | frames {job.runtime.chunk_start_frame_runtime}-{job.runtime.chunk_end_frame_runtime} | attempt {job.runtime.chunk_attempt_runtime}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}\n",
        )
    payload = window.render_session.build_render_worker_payload(job)
    if payload is None or not window._start_render_worker_payload(payload):
        window._append_log("Stderr", "[Queue] Failed to start render worker.\n")
        job.runtime.status = JobStatus.FAILED
        job.runtime.exit_code = -1
        job.runtime.finished_at = datetime.now()
        job.runtime.error_summary = "Failed to start render worker."
        window._close_current_job_log()
        window.current_job_id = None
        window._refresh_queue_table(select_job_id=job.id)
        window._maybe_start_next_job()
        return

    window._refresh_queue_table(select_job_id=job.id)
    window._set_status_message(f"Running: {job.display_name()}")


def handle_render_worker_message(window: Any, message: dict[str, Any]) -> None:
    request_id = str(message.get("request_id", "") or "")
    if request_id and window._active_render_request_id and request_id != window._active_render_request_id:
        return
    payload = dict(message.get("payload", {}) or {})
    message_type = str(message.get("type", "") or "")
    if message_type == "render.started":
        window._render_finished_message_received = False
        try:
            window._active_hbatch_pid = int(payload.get("pid", 0) or 0)
        except (TypeError, ValueError):
            window._active_hbatch_pid = 0
        return
    if message_type == "render.output":
        text = str(payload.get("text", "") or "")
        stream = str(payload.get("stream", "stdout") or "stdout")
        if not text:
            return
        window._append_log("Stderr" if stream == "stderr" else "Stdout", text)
        window._write_job_log(text)
        update_job_progress_from_output(window, text)
        return
    if message_type == "render.finished":
        window._render_finished_message_received = True
        window._active_render_request_id = ""
        normal_exit_value = int(
            getattr(QtCore.QProcess.ExitStatus.NormalExit, "value", QtCore.QProcess.ExitStatus.NormalExit)
        )
        on_render_finished(
            window,
            int(payload.get("exit_code", -1)),
            QtCore.QProcess.ExitStatus(int(payload.get("exit_status", normal_exit_value))),
        )
        return
    if message_type == "render.crashed":
        reason = str(payload.get("reason", "") or "Render worker reported a crash.")
        process_error = payload.get("process_error")
        suffix = f" ({process_error})" if process_error not in (None, "") else ""
        handle_render_worker_crash(window, f"{reason}{suffix}")


def handle_render_worker_crash(window: Any, reason: str) -> None:
    if window._pending_kill_timer is not None:
        window._pending_kill_timer.stop()
    window._kill_active_hbatch_tree()
    job = window._current_job()
    if job is None:
        return
    crash_result = window.render_session.handle_worker_crash(
        job,
        reason,
        canceling_current_job=window.canceling_current_job,
        stop_requested=window.stop_requested,
        retry_delay_value=window._retry_delay_value(),
    )
    if crash_result.terminal_finish:
        if not window.canceling_current_job and not window.stop_requested:
            window.render_session.finalize_worker_crash(job, reason)
            finished_job_id = job.id
            try:
                window._queue_next_search_index = window.jobs.index(job) + 1
            except ValueError:
                window._queue_next_search_index = 0
            window.current_job_id = None
            window._active_hbatch_pid = 0
            window.canceling_current_job = False
            window._close_current_job_log()
            window._save_queue_state()
            window._refresh_queue_table(select_job_id=finished_job_id)
            window._maybe_start_next_job()
            return
        on_render_finished(window, -1, QtCore.QProcess.ExitStatus.CrashExit)
        return
    if crash_result.retry_scheduled:
        _schedule_job_continuation(window, job, crash_result.delay_ms)


def update_job_progress_from_output(window: Any, text: str) -> None:
    job = window._current_job()
    if job is None:
        return
    window.render_session.handle_worker_output(job, text)


def on_render_finished(window: Any, exit_code: int, exit_status: QtCore.QProcess.ExitStatus) -> None:
    if window._pending_kill_timer is not None:
        window._pending_kill_timer.stop()
    window._active_hbatch_pid = 0
    job = window._current_job()
    if job is None:
        return

    was_canceled = bool(window.canceling_current_job or window.stop_requested)
    finish_result = window.render_session.handle_render_finished(
        job,
        exit_code,
        exit_status,
        was_canceled=was_canceled,
        advance_job_to_next_chunk=window._advance_job_to_next_chunk,
        retry_delay_value=window._retry_delay_value(),
    )
    window._active_render_request_id = ""
    if finish_result.continue_next_chunk:
        _schedule_job_continuation(window, job, 0)
        return
    if finish_result.retry_current_chunk:
        _schedule_job_continuation(window, job, finish_result.delay_ms)
        return
    window._clear_job_resume_runtime_state(job)
    job.runtime.chunk_start_frame_runtime = None
    job.runtime.chunk_end_frame_runtime = None
    job.runtime.chunk_step_runtime = None
    job.runtime.chunk_ranges_runtime.clear()
    job.runtime.chunk_index_runtime = 0
    job.runtime.chunk_total_runtime = 0
    job.runtime.chunk_attempt_runtime = 0
    finished_job_id = job.id
    try:
        window._queue_next_search_index = window.jobs.index(job) + 1
    except ValueError:
        window._queue_next_search_index = 0
    window.current_job_id = None
    window.canceling_current_job = False

    if window.stop_requested:
        window._save_queue_state()
        window._finish_queue("Queue stopped")
    else:
        window._save_queue_state()
        window._refresh_queue_table(select_job_id=finished_job_id)
        window._maybe_start_next_job()
