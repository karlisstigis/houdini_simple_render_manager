from __future__ import annotations

from typing import Any


def normalize_path_sync_job_ids(job_ids: list[str]) -> list[str]:
    return [str(job_id or "").strip() for job_id in job_ids if str(job_id or "").strip()]


def is_job_path_sync_locked(lock_counts: dict[str, int], job: Any) -> bool:
    if job is None:
        return False
    job_id = str(job if isinstance(job, str) else getattr(job, "id", "") or "").strip()
    if not job_id:
        return False
    return bool(lock_counts.get(job_id, 0))


def begin_path_sync_lock(lock_counts: dict[str, int], job_ids: list[str]) -> tuple[list[str], bool]:
    locked_ids = normalize_path_sync_job_ids(job_ids)
    if not locked_ids:
        return [], False
    started_overlay = not lock_counts
    for job_id in locked_ids:
        lock_counts[job_id] = int(lock_counts.get(job_id, 0)) + 1
    return locked_ids, started_overlay


def end_path_sync_lock(lock_counts: dict[str, int], job_ids: list[str]) -> tuple[list[str], bool]:
    locked_ids = normalize_path_sync_job_ids(job_ids)
    if not locked_ids:
        return [], False
    changed_ids: list[str] = []
    for job_id in locked_ids:
        count = int(lock_counts.get(job_id, 0))
        if count <= 1:
            lock_counts.pop(job_id, None)
        else:
            lock_counts[job_id] = count - 1
        changed_ids.append(job_id)
    stopped_overlay = not lock_counts
    return changed_ids, stopped_overlay


def advance_path_sync_overlay(lock_counts: dict[str, int], progress: float, *, step: float = 0.0225) -> tuple[float, bool]:
    if not lock_counts:
        return 0.0, False
    return (float(progress) + float(step)) % 1.0, True
