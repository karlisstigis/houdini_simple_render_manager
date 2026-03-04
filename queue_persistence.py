from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from atomic_io import read_json_file, write_json_atomic
from queue_models import DeviceOverrideMode, FrameHandlingMode, JobStatus, RenderJob, UsdOutputDirectoryMode


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def job_to_persisted_dict(job: RenderJob) -> dict[str, Any]:
    return {
        "id": job.spec.id,
        "hip_path": job.spec.hip_path,
        "rop_path": job.spec.rop_path,
        "frame_range_mode": job.spec.frame_range_mode,
        "start_frame": job.spec.start_frame,
        "end_frame": job.spec.end_frame,
        "step": job.spec.step,
        "name": job.spec.name,
        "status": job.runtime.status.value,
        "enabled": job.spec.enabled,
        "frame_handling_mode": job.spec.frame_handling_mode.value,
        "device_override_mode": job.spec.device_override_mode.value,
        "device_selection": job.spec.device_selection,
        "render_all_frames_single_process": job.spec.render_all_frames_single_process,
        "retain_built_usd": job.spec.retain_built_usd,
        "reuse_retained_usd": job.spec.reuse_retained_usd,
        "usd_output_directory_mode": job.spec.usd_output_directory_mode.value,
        "usd_output_directory_custom_path": job.spec.usd_output_directory_custom_path,
        "exit_code": job.runtime.exit_code,
        "log_file_path": job.runtime.log_file_path,
        "error_summary": job.runtime.error_summary,
        "interrupted_reason": job.runtime.interrupted_reason,
        "runtime_start_frame": job.runtime.runtime_start_frame,
        "runtime_end_frame": job.runtime.runtime_end_frame,
        "runtime_step": job.runtime.runtime_step,
        "rop_default_start_frame": job.runtime.rop_default_start_frame,
        "rop_default_end_frame": job.runtime.rop_default_end_frame,
        "rop_default_step": job.runtime.rop_default_step,
        "chunk_start_frame_runtime": job.runtime.chunk_start_frame_runtime,
        "chunk_end_frame_runtime": job.runtime.chunk_end_frame_runtime,
        "chunk_step_runtime": job.runtime.chunk_step_runtime,
        "chunk_index_runtime": job.runtime.chunk_index_runtime,
        "chunk_total_runtime": job.runtime.chunk_total_runtime,
        "chunk_attempt_runtime": job.runtime.chunk_attempt_runtime,
        "chunk_retry_count_runtime": job.runtime.chunk_retry_count_runtime,
        "chunk_ranges_runtime": [list(rng) for rng in list(job.runtime.chunk_ranges_runtime or [])],
        "chunk_retry_total_failures_runtime": job.runtime.chunk_retry_total_failures_runtime,
        "retained_usd_path": job.runtime.retained_usd_path,
        "retained_usd_exists": job.runtime.retained_usd_exists,
        "retained_usd_reusable": job.runtime.retained_usd_reusable,
        "retained_usd_verified": job.runtime.retained_usd_verified,
        "retained_usd_build_start_frame": job.runtime.retained_usd_build_start_frame,
        "retained_usd_build_end_frame": job.runtime.retained_usd_build_end_frame,
        "retained_usd_build_step": job.runtime.retained_usd_build_step,
        "allframesatonce_enabled": job.runtime.allframesatonce_enabled,
        "build_pass_completed": job.view.build_pass_completed,
        "strict_frame_range": job.spec.strict_frame_range,
        "offline_previous_status": job.runtime.offline_previous_status.value if job.runtime.offline_previous_status else None,
        "phase_text": job.view.phase_text,
        "progress_text": job.view.progress_text,
        "percent_text": job.view.percent_text,
        "usd_build_percent": job.view.usd_build_percent,
        "last_frame_seen": job.view.last_frame_seen,
        "prev_frame_time_text": job.view.prev_frame_time_text,
        "avg_frame_time_text": job.view.avg_frame_time_text,
        "est_job_time_text": job.view.est_job_time_text,
        "out_path": job.view.out_path,
        "out_file_sample_path": job.view.out_file_sample_path,
        "created_at": job.runtime.created_at.isoformat() if job.runtime.created_at else None,
        "started_at": job.runtime.started_at.isoformat() if job.runtime.started_at else None,
        "finished_at": job.runtime.finished_at.isoformat() if job.runtime.finished_at else None,
    }


def job_from_persisted_dict(data: dict[str, Any], *, active_job_id: str | None = None) -> RenderJob | None:
    try:
        status_text = str(data.get("status", JobStatus.QUEUED.value))
        try:
            status = JobStatus(status_text)
        except Exception:
            status = JobStatus.QUEUED
        raw_id = str(data.get("id", "") or "").strip()
        persisted_allframes = data.get("render_all_frames_single_process")
        runtime_allframes = data.get("allframesatonce_enabled")
        marked_active = bool(raw_id and active_job_id and raw_id == str(active_job_id or "").strip())
        if status == JobStatus.RUNNING or marked_active:
            status = JobStatus.INTERRUPTED

        job = RenderJob(
            hip_path=str(data.get("hip_path", "")).strip(),
            rop_path=str(data.get("rop_path", "")).strip(),
            frame_range_mode=str(data.get("frame_range_mode", "use_rop")),
            start_frame=data.get("start_frame"),
            end_frame=data.get("end_frame"),
            step=data.get("step"),
            name=str(data.get("name", "") or ""),
            status=status,
            enabled=bool(data.get("enabled", True)),
            frame_handling_mode=FrameHandlingMode.coerce(data.get("frame_handling_mode")),
            device_override_mode=DeviceOverrideMode.coerce(data.get("device_override_mode")),
            device_selection=str(data.get("device_selection", "") or ""),
            render_all_frames_single_process=(
                bool(persisted_allframes)
                if persisted_allframes is not None
                else bool(runtime_allframes) if isinstance(runtime_allframes, bool) else False
            ),
            retain_built_usd=bool(data.get("retain_built_usd", False)),
            reuse_retained_usd=bool(data.get("reuse_retained_usd", False)),
            usd_output_directory_mode=UsdOutputDirectoryMode.coerce(data.get("usd_output_directory_mode")),
            usd_output_directory_custom_path=str(data.get("usd_output_directory_custom_path", "") or ""),
        )
        if not job.spec.hip_path or not job.spec.rop_path:
            return None
        if raw_id:
            job.spec.id = raw_id

        job.runtime.exit_code = data.get("exit_code")
        job.runtime.log_file_path = str(data.get("log_file_path", "") or "")
        job.runtime.error_summary = str(data.get("error_summary", "") or "")
        job.runtime.interrupted_reason = str(data.get("interrupted_reason", "") or "")
        job.runtime.runtime_start_frame = data.get("runtime_start_frame")
        job.runtime.runtime_end_frame = data.get("runtime_end_frame")
        job.runtime.runtime_step = data.get("runtime_step")
        job.runtime.rop_default_start_frame = data.get("rop_default_start_frame")
        job.runtime.rop_default_end_frame = data.get("rop_default_end_frame")
        job.runtime.rop_default_step = data.get("rop_default_step")
        # Migration for older queue payloads that predate persistent ROP defaults.
        # Use stored runtime range only to seed missing defaults at load time.
        if job.runtime.rop_default_start_frame is None and job.runtime.runtime_start_frame is not None:
            job.runtime.rop_default_start_frame = job.runtime.runtime_start_frame
        if job.runtime.rop_default_end_frame is None and job.runtime.runtime_end_frame is not None:
            job.runtime.rop_default_end_frame = job.runtime.runtime_end_frame
        if job.runtime.rop_default_step is None and job.runtime.runtime_step not in (None, 0):
            job.runtime.rop_default_step = job.runtime.runtime_step
        job.runtime.chunk_start_frame_runtime = data.get("chunk_start_frame_runtime")
        job.runtime.chunk_end_frame_runtime = data.get("chunk_end_frame_runtime")
        job.runtime.chunk_step_runtime = data.get("chunk_step_runtime")
        job.runtime.chunk_index_runtime = int(data.get("chunk_index_runtime", 0) or 0)
        job.runtime.chunk_total_runtime = int(data.get("chunk_total_runtime", 0) or 0)
        job.runtime.chunk_attempt_runtime = int(data.get("chunk_attempt_runtime", 0) or 0)
        job.runtime.chunk_retry_count_runtime = int(data.get("chunk_retry_count_runtime", 0) or 0)
        chunk_ranges = list(data.get("chunk_ranges_runtime", []) or [])
        job.runtime.chunk_ranges_runtime = [
            (int(r[0]), int(r[1]), int(r[2]))
            for r in chunk_ranges
            if isinstance(r, (list, tuple)) and len(r) == 3
        ]
        job.runtime.chunk_retry_total_failures_runtime = int(data.get("chunk_retry_total_failures_runtime", 0) or 0)
        job.runtime.retained_usd_path = str(data.get("retained_usd_path", "") or "")
        job.runtime.retained_usd_exists = bool(data.get("retained_usd_exists", False))
        job.runtime.retained_usd_reusable = bool(data.get("retained_usd_reusable", False))
        job.runtime.retained_usd_verified = bool(data.get("retained_usd_verified", False))
        job.runtime.retained_usd_build_start_frame = _optional_int(data.get("retained_usd_build_start_frame"))
        job.runtime.retained_usd_build_end_frame = _optional_int(data.get("retained_usd_build_end_frame"))
        job.runtime.retained_usd_build_step = _optional_int(data.get("retained_usd_build_step"))
        if not job.runtime.retained_usd_verified:
            job.runtime.retained_usd_path = ""
            job.runtime.retained_usd_exists = False
            job.runtime.retained_usd_reusable = False
        afa = data.get("allframesatonce_enabled")
        job.runtime.allframesatonce_enabled = bool(afa) if isinstance(afa, bool) else None
        job.view.build_pass_completed = bool(data.get("build_pass_completed", False))
        job.spec.strict_frame_range = bool(data.get("strict_frame_range", False))
        prev_offline = data.get("offline_previous_status")
        if isinstance(prev_offline, str) and prev_offline.strip():
            try:
                job.runtime.offline_previous_status = JobStatus(prev_offline)
            except Exception:
                job.runtime.offline_previous_status = None
        job.view.phase_text = str(data.get("phase_text", "") or "")
        if job.runtime.allframesatonce_enabled is False and job.view.phase_text == "USD Build":
            job.view.phase_text = ""
        default_progress = "Done" if job.runtime.status == JobStatus.DONE else ("Canceled" if job.runtime.status == JobStatus.CANCELED else ("Interrupted" if job.runtime.status == JobStatus.INTERRUPTED else "-"))
        job.view.progress_text = str(data.get("progress_text", default_progress) or default_progress)
        job.view.percent_text = str(data.get("percent_text", "-") or "-")
        ubp = data.get("usd_build_percent")
        job.view.usd_build_percent = int(ubp) if isinstance(ubp, (int, float)) else None
        job.view.last_frame_seen = data.get("last_frame_seen")
        job.view.prev_frame_time_text = str(data.get("prev_frame_time_text", "-") or "-")
        job.view.avg_frame_time_text = str(data.get("avg_frame_time_text", "-") or "-")
        job.view.est_job_time_text = str(data.get("est_job_time_text", "-") or "-")
        job.view.out_path = str(data.get("out_path", "") or "")
        job.view.out_file_sample_path = str(data.get("out_file_sample_path", "") or "")
        job.view.render_frame_started_at = {}
        job.view.render_frame_durations_sec = []
        job.view.render_completed_frames = set()

        for attr in ("created_at", "started_at", "finished_at"):
            raw = data.get(attr)
            if isinstance(raw, str) and raw.strip():
                try:
                    setattr(job.runtime, attr, datetime.fromisoformat(raw))
                except Exception:
                    pass
        if job.runtime.status == JobStatus.INTERRUPTED:
            if not job.runtime.interrupted_reason:
                if marked_active:
                    job.runtime.interrupted_reason = "App closed or crashed while this job was active."
                else:
                    job.runtime.interrupted_reason = "Recovered from a stale running state."
            if job.runtime.chunk_total_runtime > 0:
                chunk_label = f" chunk {max(1, job.runtime.chunk_index_runtime + 1)}/{max(1, job.runtime.chunk_total_runtime)}"
                if chunk_label not in job.runtime.interrupted_reason:
                    job.runtime.interrupted_reason = f"{job.runtime.interrupted_reason} Last active:{chunk_label}."
            if not job.runtime.error_summary:
                job.runtime.error_summary = job.runtime.interrupted_reason
        return job
    except Exception:
        return None


def queue_view_to_persisted_dict(table: Any) -> dict[str, Any]:
    return {
        "column_widths": {
            str(logical): int(table.columnWidth(logical))
            for logical in range(table.columnCount())
        },
        "hidden_columns": [
            logical for logical in range(table.columnCount()) if table.isColumnHidden(logical)
        ],
    }


def load_queue_payload(path: Path) -> dict[str, Any]:
    raw = read_json_file(path)
    return raw if isinstance(raw, dict) else {}


def save_queue_payload(path: Path, *, jobs: list[RenderJob], queue_view: dict[str, Any], active_job_id: str | None = None) -> None:
    payload = {
        "version": 1,
        "saved_at": datetime.now().isoformat(),
        "active_job_id": str(active_job_id or ""),
        "queue_view": dict(queue_view or {}),
        "jobs": [job_to_persisted_dict(job) for job in jobs],
    }
    write_json_atomic(path, payload)


def job_states_for_ids(jobs: list[RenderJob], job_ids: list[str]) -> list[dict[str, Any]]:
    ordered_ids = [job_id for job_id in job_ids if job_id]
    if not ordered_ids:
        return []
    by_id = {job.id: job_to_persisted_dict(job) for job in jobs}
    return [by_id[job_id] for job_id in ordered_ids if job_id in by_id]


def remove_jobs_by_ids(jobs: list[RenderJob], job_ids: list[str]) -> list[RenderJob]:
    remove_set = {job_id for job_id in job_ids if job_id}
    if not remove_set:
        return list(jobs)
    return [job for job in jobs if job.id not in remove_set]


def insert_jobs_from_entries(jobs: list[RenderJob], entries: list[dict[str, Any]]) -> list[RenderJob]:
    new_jobs = list(jobs)
    for entry in sorted(entries, key=lambda e: int(e.get("index", 0))):
        job = job_from_persisted_dict(entry.get("job", {}))
        if job is None:
            continue
        index = max(0, min(len(new_jobs), int(entry.get("index", len(new_jobs)))))
        new_jobs.insert(index, job)
    return new_jobs


def apply_job_states(jobs: list[RenderJob], states: list[dict[str, Any]]) -> list[RenderJob]:
    new_jobs = list(jobs)
    index_by_id = {job.id: idx for idx, job in enumerate(new_jobs)}
    for state in states:
        job_id = str(state.get("id", "") or "").strip()
        if not job_id or job_id not in index_by_id:
            continue
        job = job_from_persisted_dict(state)
        if job is None:
            continue
        new_jobs[index_by_id[job_id]] = job
    return new_jobs


def apply_job_order(jobs: list[RenderJob], ordered_ids: list[str]) -> list[RenderJob]:
    if not ordered_ids:
        return list(jobs)
    current_by_id = {job.id: job for job in jobs}
    ordered_set = set(ordered_ids)
    new_jobs = [current_by_id[job_id] for job_id in ordered_ids if job_id in current_by_id]
    remaining = [job for job in jobs if job.id not in ordered_set]
    return new_jobs + remaining
