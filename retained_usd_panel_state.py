from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from queue_core.queue_models import RenderJob


def retained_usd_panel_default_fields() -> dict[str, Any]:
    return {
        "retained_usd_path": "",
        "retained_usd_built_range": "-",
        "retained_usd_built_step": "-",
        "retained_usd_built_at": "-",
        "retained_usd_status": "-",
        "retained_usd_warning": "",
        "can_open": False,
        "can_delete": False,
    }


def single_job_retained_usd_panel_state(
    job: RenderJob,
    *,
    sync_file_state: Callable[[RenderJob], None],
    load_metadata: Callable[[Path], dict[str, Any] | None],
    build_info_text: Callable[[dict[str, Any] | None], tuple[str, str]],
    built_at_text: Callable[[dict[str, Any] | None], str],
    is_absolute_path: Callable[[str], bool],
    configured_folder_preview: Callable[[RenderJob], str],
    hip_stale_reason: Callable[[RenderJob, dict[str, Any] | None], str],
    stale_reason: Callable[[RenderJob], str],
    invalid_reason: Callable[[RenderJob], str],
    status_text: Callable[[RenderJob, dict[str, Any] | None], str],
) -> dict[str, Any]:
    sync_file_state(job)
    retained_file_path = str(job.runtime.retained_usd_path or "").strip()
    metadata: dict[str, Any] | None = None
    retained_path_text = ""
    retained_built_range_text = "-"
    retained_built_step_text = "-"
    retained_built_at_value = "-"
    can_open = False

    if retained_file_path:
        retained_path = Path(retained_file_path)
        retained_path_text = str(retained_path.parent)
        metadata = load_metadata(retained_path)
        retained_built_range_text, retained_built_step_text = build_info_text(metadata)
        retained_built_at_value = built_at_text(metadata)
        can_open = bool(job.runtime.retained_usd_exists and is_absolute_path(retained_file_path))
    elif bool(job.spec.retain_built_usd):
        retained_path_text = configured_folder_preview(job)

    retained_warning = hip_stale_reason(job, metadata) if metadata else stale_reason(job)
    if not retained_warning:
        retained_warning = invalid_reason(job)

    return {
        "retained_usd_path": retained_path_text,
        "retained_usd_built_range": retained_built_range_text,
        "retained_usd_built_step": retained_built_step_text,
        "retained_usd_built_at": retained_built_at_value,
        "retained_usd_status": status_text(job, metadata),
        "retained_usd_warning": retained_warning,
        "can_open": can_open,
    }


def multi_job_retained_usd_panel_state(paths: list[Path]) -> dict[str, Any]:
    return {
        "retained_usd_path": f"{len({str(path.parent) for path in paths})} USD folder(s)" if paths else "",
        "retained_usd_built_range": "-",
        "retained_usd_built_step": "-",
        "retained_usd_built_at": "-",
        "retained_usd_status": f"{len(paths)} file(s) available" if paths else "No retained USD files",
        "retained_usd_warning": "",
        "can_open": False,
    }


def can_delete_retained_usd(
    *,
    selected_count: int,
    retained_state_can_open: bool,
    retained_paths_present: bool,
    has_active_or_locked_job: bool,
) -> bool:
    can_delete = bool(retained_state_can_open) if selected_count == 1 else bool(retained_paths_present)
    if can_delete:
        can_delete = not has_active_or_locked_job
    return can_delete
