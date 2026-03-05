from __future__ import annotations

from typing import Any, Callable

from job_properties_presenter import (
    has_active_or_locked_jobs,
    selected_jobs_editable,
    selected_jobs_summary,
    should_show_custom_devices,
)
from job_properties_state import build_job_properties_panel_state
from queue_models import RenderJob
from retained_usd_panel_state import can_delete_retained_usd, multi_job_retained_usd_panel_state


def build_job_properties_state_for_selection(
    *,
    selected_jobs: list[RenderJob],
    panel_default_state: Callable[[], dict[str, Any]],
    mixed_value: Callable[[list[Any]], tuple[bool, Any]],
    job_file_name: Callable[[RenderJob], str],
    job_rop_name: Callable[[RenderJob], str],
    single_job_retained_state: Callable[[RenderJob], dict[str, Any]],
    selected_retained_paths: Callable[[], list[Any]],
    can_edit_job_for_panel: Callable[[RenderJob], bool],
    device_option_states_for_jobs: Callable[[list[RenderJob], bool, bool], list[dict[str, Any]]],
    is_active_job: Callable[[RenderJob], bool],
    is_locked_job: Callable[[RenderJob], bool],
    unchecked_state: int,
    checked_state: int,
    partial_state: int,
    default_device_mode: str,
    default_usd_output_mode: str,
) -> dict[str, Any]:
    if not selected_jobs:
        return panel_default_state()

    summary = selected_jobs_summary(
        selected_jobs,
        mixed_value=mixed_value,
        job_file_name=job_file_name,
        job_rop_name=job_rop_name,
    )
    retained_paths: list[Any] = []
    if int(summary["selected_count"]) == 1:
        retained_usd_state = single_job_retained_state(selected_jobs[0])
    else:
        retained_paths = selected_retained_paths()
        retained_usd_state = multi_job_retained_usd_panel_state(retained_paths)

    editable = selected_jobs_editable(selected_jobs, can_edit_job=can_edit_job_for_panel)
    show_custom_devices = should_show_custom_devices(
        mixed_device_mode=bool(summary["mixed_device_mode"]),
        first_device_mode=str(summary["first_device_mode"] or ""),
    )
    can_delete = can_delete_retained_usd(
        selected_count=int(summary["selected_count"]),
        retained_state_can_open=bool(retained_usd_state["can_open"]),
        retained_paths_present=bool(retained_paths),
        has_active_or_locked_job=has_active_or_locked_jobs(
            selected_jobs,
            is_active_job=is_active_job,
            is_locked_job=is_locked_job,
        ),
    )

    return build_job_properties_panel_state(
        mixed_name=bool(summary["mixed_name"]),
        first_name=summary["first_name"],
        mixed_file=bool(summary["mixed_file"]),
        first_file=summary["first_file"],
        mixed_rop=bool(summary["mixed_rop"]),
        first_rop=summary["first_rop"],
        editable=editable,
        mixed_device_mode=bool(summary["mixed_device_mode"]),
        first_device_mode=str(summary["first_device_mode"] or ""),
        show_custom_devices=show_custom_devices,
        device_options=device_option_states_for_jobs(selected_jobs, show_custom_devices, editable),
        mixed_single_process=bool(summary["mixed_single_process"]),
        first_single_process=bool(summary["first_single_process"]),
        mixed_retain=bool(summary["mixed_retain"]),
        first_retain=bool(summary["first_retain"]),
        mixed_reuse=bool(summary["mixed_reuse"]),
        first_reuse=bool(summary["first_reuse"]),
        mixed_usd_output_mode=bool(summary["mixed_usd_output_mode"]),
        first_usd_output_mode=str(summary["first_usd_output_mode"] or ""),
        mixed_usd_output_custom_path=bool(summary["mixed_usd_output_custom_path"]),
        first_usd_output_custom_path=str(summary["first_usd_output_custom_path"] or ""),
        retained_usd_state=retained_usd_state,
        can_delete=can_delete,
        unchecked_state=unchecked_state,
        checked_state=checked_state,
        partial_state=partial_state,
        default_device_mode=default_device_mode,
        default_usd_output_mode=default_usd_output_mode,
    )
