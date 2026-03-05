from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from PySide6 import QtCore

from queue_core.queue_execution import apply_render_finished_state as apply_render_finished_state_model
from queue_core.queue_execution import retry_current_chunk as retry_current_chunk_model
from queue_core.queue_models import JobStatus, RenderJob
from render_output_parser import (
    update_job_from_hsrm_markers as update_job_from_hsrm_markers_model,
    update_job_runtime_flags_from_output as update_job_runtime_flags_from_output_model,
)
from render_runner import build_render_command_plan


@dataclass
class RenderSessionHooks:
    append_log: Callable[[str, str], None]
    write_job_log: Callable[[str], None]
    close_current_job_log: Callable[[], None]
    save_queue_state: Callable[[], bool]
    refresh_job_row: Callable[[str], None]
    refresh_queue_table: Callable[..., None]
    safe_message: Callable[[str, str, str | None], None]
    start_worker_render: Callable[[dict[str, Any]], bool]
    ensure_husk_hook_files: Callable[[], dict[str, str]]
    build_render_preflight_script: Callable[[RenderJob, bool, dict[str, str]], str]
    current_hbatch_path: Callable[[], str]
    build_render_environment: Callable[[RenderJob], dict[str, str]]
    normalize_output_display_path: Callable[[str], str]
    hscript_quote: Callable[[str], str]
    current_time: Callable[[], datetime]
    update_job_render_timing_stats: Callable[[RenderJob], None]
    update_phase_from_frame_sequence: Callable[[RenderJob, float | None], None]
    update_job_phase_from_output: Callable[[RenderJob, str], None]
    cancel_phase_promote: Callable[[], None]
    mark_job_offline: Callable[[RenderJob, str | None], None]
    sync_retained_usd_file_state: Callable[[RenderJob], None]


@dataclass
class RenderSessionResult:
    started: bool
    needs_queue_advance: bool = False
    error_message: str = ""


@dataclass
class RenderCrashResult:
    retry_scheduled: bool
    terminal_finish: bool
    delay_ms: int = 0


@dataclass
class RenderFinishResult:
    continue_next_chunk: bool
    retry_current_chunk: bool
    delay_ms: int
    final_status: JobStatus


class RenderSessionController:
    def __init__(
        self,
        hooks: RenderSessionHooks,
        *,
        hook_script_path_fn: Callable[[str], Path],
        disable_husk_mplay_fn: Callable[[], bool],
    ) -> None:
        self._hooks = hooks
        self._hook_script_path_fn = hook_script_path_fn
        self._disable_husk_mplay_fn = disable_husk_mplay_fn

    def build_render_worker_payload(self, job: RenderJob) -> dict[str, Any] | None:
        commands: list[str] = []
        disable_husk_mplay = bool(self._disable_husk_mplay_fn())
        hook_paths = self._hooks.ensure_husk_hook_files()
        if hook_paths:
            self._hooks.append_log("Stdout", "[Preflight] Husk hook files prepared (session only).\n")
        try:
            preflight_script = self._hooks.build_render_preflight_script(job, disable_husk_mplay, hook_paths)
        except Exception as exc:
            self._hooks.append_log("Stderr", f"[Preflight] {exc}\n")
            self._hooks.safe_message("Missing Houdini Scripts", str(exc), None)
            preflight_script = ""
        try:
            preflight_path = self._hook_script_path_fn("hsrm_render_preflight")
            preflight_path.write_text(preflight_script, encoding="utf-8")
            commands.append(f"python {self._hooks.hscript_quote(str(preflight_path))}")
        except Exception as exc:
            self._hooks.append_log("Stderr", f"[Preflight] Failed to write preflight script file: {exc}\n")
        try:
            plan = build_render_command_plan(job, self._hooks.hscript_quote)
        except ValueError as exc:
            self._hooks.append_log("Stderr", f"[Queue] {exc}\n")
            self._hooks.safe_message("Invalid Override Range", str(exc), None)
            return None

        if job.chunk_total_runtime > 1:
            chunk_msg = (
                f"[Chunk] {job.runtime.chunk_index_runtime + 1}/{job.runtime.chunk_total_runtime} "
                f"frames {job.runtime.chunk_start_frame_runtime}-{job.runtime.chunk_end_frame_runtime} "
                f"attempt {max(1, job.runtime.chunk_attempt_runtime)}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}\n"
            )
            self._hooks.append_log("Stdout", chunk_msg)
            self._hooks.write_job_log(chunk_msg)
        if plan.is_resume_runtime:
            resume_text = (
                f"[Resume] Starting from frame {plan.effective_start} based on existing outputs (visible queue range unchanged).\n"
            )
            self._hooks.append_log("Stdout", resume_text)
            self._hooks.write_job_log(resume_text)
        command_text = f"[Queue] Render command ({plan.command_mode}): {plan.command_text}\n"
        self._hooks.append_log("Stdout", command_text)
        self._hooks.write_job_log(command_text)
        commands.extend([plan.command_text, "quit"])
        return {
            "job_id": job.id,
            "hip_path": job.spec.hip_path,
            "hbatch_path": self._hooks.current_hbatch_path(),
            "environment": dict(self._hooks.build_render_environment(job) or {}),
            "commands": commands,
            "effective_plan": {
                "mode": plan.command_mode,
                "start": plan.effective_start,
                "end": plan.effective_end,
                "step": plan.effective_step,
                "chunk_index": int(job.runtime.chunk_index_runtime),
                "chunk_total": int(job.runtime.chunk_total_runtime),
                "attempt": int(max(1, job.runtime.chunk_attempt_runtime)),
                "retry_budget": int(max(0, job.runtime.chunk_retry_count_runtime)),
            },
        }

    def start_job_continuation(
        self,
        job: RenderJob,
        *,
        current_job_id: str | None,
        stop_requested: bool,
        canceling_current_job: bool,
    ) -> RenderSessionResult:
        if current_job_id != job.id or stop_requested or canceling_current_job:
            return RenderSessionResult(started=False)
        self._hooks.append_log("Stdout", f"\n=== Render Continue: {job.display_name()} ===\n")
        if job.chunk_total_runtime > 0:
            self._hooks.append_log(
                "Stdout",
                f"[Chunk] {max(1, job.runtime.chunk_index_runtime + 1)}/{max(1, job.runtime.chunk_total_runtime)} | "
                f"frames {job.runtime.chunk_start_frame_runtime}-{job.runtime.chunk_end_frame_runtime} | "
                f"attempt {max(1, job.runtime.chunk_attempt_runtime)}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}\n",
            )
        payload = self.build_render_worker_payload(job)
        if payload is None or not self._hooks.start_worker_render(payload):
            self._hooks.append_log("Stderr", "[Queue] Failed to start render worker (continuation).\n")
            job.runtime.status = JobStatus.FAILED
            job.runtime.finished_at = self._hooks.current_time()
            job.runtime.exit_code = -1
            job.runtime.error_summary = "Failed to start render worker."
            self._hooks.close_current_job_log()
            self._hooks.save_queue_state()
            self._hooks.refresh_queue_table(select_job_id=job.id)
            return RenderSessionResult(started=False, needs_queue_advance=True, error_message=job.runtime.error_summary)
        self._hooks.refresh_queue_table(select_job_id=job.id)
        return RenderSessionResult(started=True)

    def handle_worker_output(self, job: RenderJob, text: str) -> None:
        is_resume_run = bool(
            job.resume_start_frame_runtime is not None
            and job.resume_end_frame_runtime is not None
            and (job.resume_step_runtime or 0) > 0
        )
        is_chunk_run = bool(
            job.chunk_total_runtime > 1
            and job.chunk_start_frame_runtime is not None
            and job.chunk_end_frame_runtime is not None
            and (job.chunk_step_runtime or 0) > 0
        )
        update_job_runtime_flags_from_output_model(
            job,
            text,
            update_runtime_range=not (is_resume_run or is_chunk_run),
        )
        self._hooks.update_job_phase_from_output(job, text)

        previous_frame_seen = job.view.last_frame_seen
        update_job_from_hsrm_markers_model(
            job,
            text,
            self._hooks.normalize_output_display_path,
            self._hooks.update_job_render_timing_stats,
        )
        frame_matches = re.findall(r"(?i)\bframe\s+(-?\d+(?:\.\d+)?)\b", text)
        if frame_matches and "__HSRM_FRAME__|" not in text:
            try:
                job.view.last_frame_seen = float(frame_matches[-1])
            except ValueError:
                pass

        self._hooks.update_phase_from_frame_sequence(job, previous_frame_seen)
        if job.runtime.retained_usd_metadata_pending_write:
            self._hooks.sync_retained_usd_file_state(job)

        total: int | None = None
        range_start: float | None = None
        range_step: float | None = None
        if job.spec.frame_range_mode == "override":
            total = job.total_override_frames()
            if job.spec.start_frame is not None and job.spec.step:
                range_start = float(job.spec.start_frame)
                range_step = float(job.spec.step)
        elif job.runtime.runtime_start_frame is not None and job.runtime.runtime_end_frame is not None and job.runtime.runtime_step not in (None, 0):
            range_start = float(job.runtime.runtime_start_frame)
            range_step = float(job.runtime.runtime_step)
            total = int(((job.runtime.runtime_end_frame - job.runtime.runtime_start_frame) / job.runtime.runtime_step) + 1)

        if job.view.last_frame_seen is not None:
            job.view.progress_text = f"{job.view.last_frame_seen:g}"
        if total and total > 0 and job.view.last_frame_seen is not None and range_start is not None and range_step:
            idx = int((job.view.last_frame_seen - range_start) / range_step) + 1
            idx = max(0, min(total, idx))
            completed_idx = idx
            if job.view.phase_text == "Render":
                completed_idx = max(
                    0,
                    min(total, int(job.runtime.resume_completed_baseline_count or 0) + len(job.view.render_completed_frames)),
                )
            pct = int((completed_idx / total) * 100)
            job.view.percent_text = f"{pct}% ({completed_idx}/{total})"
            if job.view.phase_text == "USD Build":
                job.view.usd_build_percent = pct
        elif job.view.last_frame_seen is not None and not job.view.percent_text:
            job.view.percent_text = "-"

        out_match = re.search(r">>>\s*Render\s+(.+?),\s", text)
        if out_match:
            candidate = out_match.group(1).strip()
            if candidate:
                job.view.out_file_sample_path = candidate
                job.view.out_path = self._hooks.normalize_output_display_path(candidate)

        if any(k in text.lower() for k in ("error", "failed", "traceback")):
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if lines:
                job.runtime.error_summary = lines[-1][:300]
        for line in text.splitlines():
            low = line.lower()
            if "node not found for preflight" in low or "couldn't find renderer " in low:
                job.runtime.offline_detected_reason = "ROP node not found in HIP file."

        self._hooks.refresh_job_row(job.id)

    def handle_worker_crash(
        self,
        job: RenderJob,
        reason: str,
        *,
        canceling_current_job: bool,
        stop_requested: bool,
        retry_delay_value: int,
    ) -> RenderCrashResult:
        self._hooks.append_log("Stderr", f"[Render Worker] {reason}\n")
        if canceling_current_job or stop_requested:
            return RenderCrashResult(retry_scheduled=False, terminal_finish=True)
        if not job.runtime.error_summary:
            job.runtime.error_summary = reason
        if retry_current_chunk_model(job):
            delay_sec = max(0, retry_delay_value)
            self._hooks.append_log(
                "Stdout",
                f"[Retry] Worker crash retry for chunk {max(1, job.runtime.chunk_index_runtime + 1)}/{max(1, job.runtime.chunk_total_runtime)} "
                f"attempt {job.runtime.chunk_attempt_runtime}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}"
                + (f" after {delay_sec}s\n" if delay_sec > 0 else " (immediate)\n"),
            )
            job.runtime.finished_at = None
            job.runtime.exit_code = None
            job.runtime.status = JobStatus.RUNNING
            self._hooks.refresh_queue_table(select_job_id=job.id)
            return RenderCrashResult(retry_scheduled=True, terminal_finish=False, delay_ms=delay_sec * 1000)
        return RenderCrashResult(retry_scheduled=False, terminal_finish=True)

    def finalize_worker_crash(self, job: RenderJob, reason: str) -> None:
        clean_reason = str(reason or "Render worker crashed unexpectedly.").strip()
        if job.runtime.chunk_total_runtime > 0:
            chunk_label = (
                f" Last active: chunk {max(1, job.runtime.chunk_index_runtime + 1)}/"
                f"{max(1, job.runtime.chunk_total_runtime)}."
            )
            if chunk_label.strip() not in clean_reason:
                clean_reason = f"{clean_reason}{chunk_label}"
        job.runtime.status = JobStatus.INTERRUPTED
        job.runtime.finished_at = self._hooks.current_time()
        job.runtime.exit_code = -1
        job.runtime.interrupted_reason = clean_reason
        job.runtime.error_summary = clean_reason
        if not job.view.progress_text or job.view.progress_text == "-":
            job.view.progress_text = "Interrupted"
        job.view.percent_text = "-"
        self._hooks.write_job_log(f"\n=== Job Interrupted: worker crash ===\n{clean_reason}\n")

    def handle_render_finished(
        self,
        job: RenderJob,
        exit_code: int,
        exit_status: QtCore.QProcess.ExitStatus,
        *,
        was_canceled: bool,
        advance_job_to_next_chunk: Callable[[RenderJob], bool],
        retry_delay_value: int,
    ) -> RenderFinishResult:
        self._hooks.cancel_phase_promote()
        finish_eval = apply_render_finished_state_model(
            job,
            exit_code=exit_code,
            exit_status=exit_status,
            done_status=JobStatus.DONE,
            failed_status=JobStatus.FAILED,
            canceled_status=JobStatus.CANCELED,
            now_fn=self._hooks.current_time,
            mark_job_offline=self._hooks.mark_job_offline,
            was_canceled=was_canceled,
        )
        self._hooks.append_log("Stdout", f"\n=== Render End: {job.display_name()} | {job.runtime.status.value} | exit={exit_code} ===\n")
        if job.runtime.error_summary:
            self._hooks.append_log("Stderr", f"[Job Summary] {job.runtime.error_summary}\n")

        self._hooks.write_job_log(f"\n=== Job Finished: {job.runtime.status.value} (exit={exit_code}) ===\n")
        if job.runtime.error_summary:
            self._hooks.write_job_log(f"Error Summary: {job.runtime.error_summary}\n")

        if not finish_eval.was_canceled and not finish_eval.was_offline and finish_eval.logical_success and advance_job_to_next_chunk(job):
            self._hooks.append_log(
                "Stdout",
                f"[Chunk] Next {job.runtime.chunk_index_runtime + 1}/{job.runtime.chunk_total_runtime} "
                f"frames {job.runtime.chunk_start_frame_runtime}-{job.runtime.chunk_end_frame_runtime} "
                f"attempt {job.runtime.chunk_attempt_runtime}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}\n",
            )
            self._hooks.save_queue_state()
            self._hooks.refresh_queue_table(select_job_id=job.id)
            return RenderFinishResult(continue_next_chunk=True, retry_current_chunk=False, delay_ms=0, final_status=job.runtime.status)

        if not finish_eval.was_canceled and not finish_eval.was_offline and (not finish_eval.logical_success) and retry_current_chunk_model(job):
            delay_sec = max(0, retry_delay_value)
            self._hooks.append_log(
                "Stdout",
                f"[Retry] Chunk {max(1, job.runtime.chunk_index_runtime + 1)}/{max(1, job.runtime.chunk_total_runtime)} "
                f"attempt {job.runtime.chunk_attempt_runtime}/{1 + max(0, job.runtime.chunk_retry_count_runtime)}"
                + (f" after {delay_sec}s\n" if delay_sec > 0 else " (immediate)\n"),
            )
            job.runtime.finished_at = None
            job.runtime.exit_code = None
            job.runtime.status = JobStatus.RUNNING
            self._hooks.refresh_queue_table(select_job_id=job.id)
            return RenderFinishResult(continue_next_chunk=False, retry_current_chunk=True, delay_ms=delay_sec * 1000, final_status=job.runtime.status)

        if not finish_eval.was_canceled and not finish_eval.was_offline and finish_eval.logical_success:
            self._hooks.sync_retained_usd_file_state(job)
        self._hooks.close_current_job_log()
        self._hooks.save_queue_state()
        self._hooks.refresh_queue_table(select_job_id=job.id)
        return RenderFinishResult(continue_next_chunk=False, retry_current_chunk=False, delay_ms=0, final_status=job.runtime.status)
