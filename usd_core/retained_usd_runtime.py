from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable

from atomic_io import write_json_atomic
from queue_core.queue_models import JobStatus, RenderJob


def selected_retained_usd_paths(
    jobs: list[RenderJob],
    *,
    is_absolute_path: Callable[[str], bool],
) -> list[Path]:
    paths: list[Path] = []
    for job in jobs:
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        if not is_absolute_path(retained_path):
            continue
        path = Path(retained_path)
        if path.exists():
            paths.append(path)
    return paths


def is_absolute_retained_usd_path(path_text: str) -> bool:
    try:
        return bool(path_text) and Path(path_text).is_absolute()
    except (TypeError, ValueError, OSError):
        return False


def clear_retained_usd_runtime(job: RenderJob) -> None:
    job.runtime.retained_usd_path = ""
    job.runtime.retained_usd_exists = False
    job.runtime.retained_usd_reusable = False
    job.runtime.retained_usd_verified = False
    job.runtime.retained_usd_build_start_frame = None
    job.runtime.retained_usd_build_end_frame = None
    job.runtime.retained_usd_build_step = None
    job.runtime.retained_usd_metadata_pending_write = False


def should_write_retained_usd_metadata_now(job: RenderJob) -> bool:
    if job.runtime.status == JobStatus.DONE:
        return True
    return bool(
        job.runtime.status == JobStatus.RUNNING
        and job.view.build_pass_completed
        and job.view.phase_text == "Render"
    )


def write_retained_usd_metadata(
    job: RenderJob,
    retained_usd_path: Path,
    *,
    metadata_path_for: Callable[[Path], Path],
    append_log: Callable[[str, str], None],
    now_fn: Callable[[], datetime] = datetime.now,
) -> None:
    if (
        job.runtime.retained_usd_build_start_frame is None
        or job.runtime.retained_usd_build_end_frame is None
        or (job.runtime.retained_usd_build_step or 0) <= 0
    ):
        return
    start = int(job.runtime.retained_usd_build_start_frame)
    end = int(job.runtime.retained_usd_build_end_frame)
    step = int(job.runtime.retained_usd_build_step)
    try:
        hip_mtime = Path(job.spec.hip_path).stat().st_mtime if str(job.spec.hip_path or "").strip() else None
    except OSError:
        hip_mtime = None
    payload = {
        "version": 1,
        "hip_path": str(job.spec.hip_path or ""),
        "hip_mtime": hip_mtime,
        "rop_path": str(job.spec.rop_path or ""),
        "start_frame": int(start),
        "end_frame": int(end),
        "step": int(step),
        "built_at": now_fn().isoformat(timespec="seconds"),
    }
    try:
        write_json_atomic(metadata_path_for(retained_usd_path), payload)
    except (OSError, TypeError, ValueError) as exc:
        append_log("Stderr", f"[RetainUSD] Failed to write metadata sidecar: {exc}\n")


def sync_retained_usd_file_state(
    job: RenderJob,
    *,
    invalid_reason_for_job: Callable[[RenderJob], str],
    should_write_metadata_now: Callable[[RenderJob], bool],
    write_metadata: Callable[[RenderJob, Path], None],
) -> None:
    if not bool(job.runtime.retained_usd_verified):
        job.runtime.retained_usd_path = ""
        job.runtime.retained_usd_exists = False
        job.runtime.retained_usd_reusable = False
        return
    retained_path = str(job.runtime.retained_usd_path or "").strip()
    if not retained_path:
        job.runtime.retained_usd_exists = False
        job.runtime.retained_usd_reusable = False
        return
    path = Path(retained_path)
    exists = path.exists()
    job.runtime.retained_usd_exists = exists
    if (
        exists
        and bool(job.runtime.retained_usd_metadata_pending_write)
        and should_write_metadata_now(job)
    ):
        write_metadata(job, path)
        job.runtime.retained_usd_metadata_pending_write = False
    job.runtime.retained_usd_reusable = bool(exists and not invalid_reason_for_job(job))


def delete_retained_usd_folder_for_job(
    job: RenderJob,
    *,
    is_absolute_path: Callable[[str], bool],
    clear_runtime: Callable[[RenderJob], None],
    append_log: Callable[[str, str], None],
) -> bool:
    retained_path = str(job.runtime.retained_usd_path or "").strip()
    if not retained_path:
        return False
    if not is_absolute_path(retained_path):
        append_log("Stderr", f"[RetainUSD] Ignoring non-absolute retained USD path: {retained_path}\n")
        clear_runtime(job)
        return False
    try:
        folder = Path(retained_path).resolve().parent
    except (OSError, RuntimeError):
        folder = Path(retained_path).parent
    if not folder.exists():
        clear_runtime(job)
        return False
    try:
        import shutil

        shutil.rmtree(folder, ignore_errors=False)
    except OSError as exc:
        append_log("Stderr", f"[RetainUSD] Failed to delete previous USD folder before rebuild: {folder} ({exc})\n")
        return False
    clear_runtime(job)
    append_log("Stdout", f"[RetainUSD] Deleted previous USD folder before rebuild: {folder}\n")
    return True
