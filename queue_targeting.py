from __future__ import annotations

from queue_models import RenderJob


def selection_ids_for_refresh(
    selected_ids: list[str],
    fallback_job_ids: list[str] | None = None,
) -> list[str] | None:
    if selected_ids:
        return list(selected_ids)
    if fallback_job_ids:
        values = [job_id for job_id in fallback_job_ids if job_id]
        return values or None
    return None


def tree_context_target_jobs(
    jobs: list[RenderJob],
    *,
    hip_path: str,
    rop_path: str,
    kind: str,
) -> list[RenderJob]:
    hip_value = str(hip_path or "").strip()
    rop_value = str(rop_path or "").strip()
    kind_value = str(kind or "").strip().lower()
    if not hip_value:
        return []
    if kind_value == "rop" and rop_value:
        return [job for job in jobs if job.spec.hip_path == hip_value and job.spec.rop_path == rop_value]
    return [job for job in jobs if job.spec.hip_path == hip_value]


def selected_job_for_row(jobs: list[RenderJob], row: int) -> RenderJob | None:
    if 0 <= row < len(jobs):
        return jobs[row]
    return None
