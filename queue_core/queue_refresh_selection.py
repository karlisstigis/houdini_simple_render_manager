from __future__ import annotations


def preserved_selection(
    *,
    select_row: int | None,
    select_job_id: str | None,
    select_job_ids: list[str] | None,
    current_selected_job_ids: list[str],
    current_selected_job_id: str | None,
) -> tuple[list[str], str | None]:
    if select_row is None and select_job_id is None and not select_job_ids:
        if current_selected_job_ids:
            return list(current_selected_job_ids), None
        if current_selected_job_id:
            return [], str(current_selected_job_id)
    return [], None


def target_selection(
    *,
    select_job_id: str | None,
    select_job_ids: list[str] | None,
    preserved_job_id: str | None,
    preserved_job_ids: list[str],
) -> tuple[str | None, list[str], set[str]]:
    target_job_id = select_job_id or preserved_job_id
    target_job_ids = list(select_job_ids or preserved_job_ids)
    return target_job_id, target_job_ids, set(target_job_ids)


def clamped_select_row(select_row: int | None, *, job_count: int) -> int | None:
    if select_row is None or job_count <= 0:
        return None
    return max(0, min(int(select_row), int(job_count) - 1))
