from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from queue_core.queue_models import JobStatus, RenderJob


def write_queue_snapshot(
    *,
    base_dir: Path,
    reason: str,
    jobs: list[RenderJob],
    queue_view: dict[str, Any],
    active_job_id: str | None,
    save_queue_payload_fn: Callable[..., None],
    max_files: int = 5,
) -> Path:
    backups_dir = Path(base_dir) / "queue_backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    safe_reason = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(reason or "").strip().lower()).strip("_") or "snapshot"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    snapshot_path = backups_dir / f"queue_{stamp}_{safe_reason}.json"
    save_queue_payload_fn(
        snapshot_path,
        jobs=jobs,
        queue_view=queue_view,
        active_job_id=active_job_id,
    )
    snapshots = sorted(backups_dir.glob("queue_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for stale in snapshots[max(1, int(max_files)):]:
        try:
            stale.unlink()
        except OSError:
            pass
    return snapshot_path


def build_queue_run_summary(jobs: list[RenderJob], started_job_ids: set[str]) -> tuple[str, str] | None:
    if not started_job_ids:
        return None
    started_jobs = [job for job in jobs if job.id in started_job_ids]
    if not started_jobs:
        return None
    counts = {
        JobStatus.DONE: 0,
        JobStatus.FAILED: 0,
        JobStatus.CANCELED: 0,
        JobStatus.INTERRUPTED: 0,
        JobStatus.OFFLINE: 0,
        JobStatus.RUNNING: 0,
        JobStatus.QUEUED: 0,
    }
    frame_times: list[float] = []
    for job in started_jobs:
        status = job.runtime.status
        counts[status] = counts.get(status, 0) + 1
        for sec in list(job.view.render_frame_durations_sec or []):
            try:
                v = float(sec)
            except (TypeError, ValueError):
                continue
            if v >= 0:
                frame_times.append(v)
    avg_frame = ""
    if frame_times:
        avg_frame = f" | Avg frame {sum(frame_times)/len(frame_times):.2f}s"
    message = (
        f"Run summary: {len(started_jobs)} job(s) | "
        f"Done {counts.get(JobStatus.DONE, 0)} | "
        f"Failed {counts.get(JobStatus.FAILED, 0)} | "
        f"Canceled {counts.get(JobStatus.CANCELED, 0)} | "
        f"Interrupted {counts.get(JobStatus.INTERRUPTED, 0)}"
        f"{avg_frame}"
    )
    severity = "warning" if (
        counts.get(JobStatus.FAILED, 0)
        or counts.get(JobStatus.CANCELED, 0)
        or counts.get(JobStatus.INTERRUPTED, 0)
    ) else "info"
    return message, severity
