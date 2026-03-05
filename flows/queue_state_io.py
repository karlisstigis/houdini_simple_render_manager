from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def load_queue_state(
    path: Path,
    *,
    load_queue_payload_fn: Callable[[Path], dict[str, Any]],
    job_from_persisted_dict_fn: Callable[[dict[str, Any], str], Any],
) -> tuple[list[Any], dict[str, Any], str]:
    raw = load_queue_payload_fn(path)
    jobs_data = raw.get("jobs", [])
    queue_view = raw.get("queue_view", {})
    active_job_id = str(raw.get("active_job_id", "") or "").strip()
    loaded_jobs: list[Any] = []
    if isinstance(jobs_data, list):
        for item in jobs_data:
            if not isinstance(item, dict):
                continue
            job = job_from_persisted_dict_fn(item, active_job_id)
            if job is not None:
                loaded_jobs.append(job)
    return loaded_jobs, queue_view if isinstance(queue_view, dict) else {}, active_job_id


def save_queue_state(
    *,
    current_queue_path: Path,
    path_override: Path | None,
    jobs: list[Any],
    queue_view: dict[str, Any],
    active_job_id: str,
    save_queue_payload_fn: Callable[..., None],
) -> Path:
    target_path = path_override or current_queue_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    save_queue_payload_fn(
        target_path,
        jobs=jobs,
        queue_view=queue_view,
        active_job_id=active_job_id,
    )
    return target_path
