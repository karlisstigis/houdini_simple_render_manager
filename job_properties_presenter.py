from __future__ import annotations

from typing import Any, Callable

from queue_core.queue_models import DeviceOverrideMode, RenderJob


def selected_jobs_summary(
    selected_jobs: list[RenderJob],
    *,
    mixed_value: Callable[[list[Any]], tuple[bool, Any]],
    job_file_name: Callable[[RenderJob], str],
    job_rop_name: Callable[[RenderJob], str],
) -> dict[str, Any]:
    mixed_name, first_name = mixed_value([job.display_name() for job in selected_jobs])
    mixed_file, first_file = mixed_value([job_file_name(job) for job in selected_jobs])
    mixed_rop, first_rop = mixed_value([job_rop_name(job) for job in selected_jobs])
    mixed_device_mode, first_device_mode = mixed_value([job.spec.device_override_mode.value for job in selected_jobs])
    mixed_single_process, first_single_process = mixed_value([bool(job.spec.render_all_frames_single_process) for job in selected_jobs])
    mixed_retain, first_retain = mixed_value([bool(job.spec.retain_built_usd) for job in selected_jobs])
    mixed_reuse, first_reuse = mixed_value([bool(job.spec.reuse_retained_usd) for job in selected_jobs])
    mixed_usd_output_mode, first_usd_output_mode = mixed_value([job.spec.usd_output_directory_mode.value for job in selected_jobs])
    mixed_usd_output_custom_path, first_usd_output_custom_path = mixed_value([str(job.spec.usd_output_directory_custom_path or "") for job in selected_jobs])
    return {
        "selected_count": len(selected_jobs),
        "mixed_name": mixed_name,
        "first_name": first_name,
        "mixed_file": mixed_file,
        "first_file": first_file,
        "mixed_rop": mixed_rop,
        "first_rop": first_rop,
        "mixed_device_mode": mixed_device_mode,
        "first_device_mode": first_device_mode,
        "mixed_single_process": mixed_single_process,
        "first_single_process": bool(first_single_process),
        "mixed_retain": mixed_retain,
        "first_retain": bool(first_retain),
        "mixed_reuse": mixed_reuse,
        "first_reuse": bool(first_reuse),
        "mixed_usd_output_mode": mixed_usd_output_mode,
        "first_usd_output_mode": first_usd_output_mode,
        "mixed_usd_output_custom_path": mixed_usd_output_custom_path,
        "first_usd_output_custom_path": str(first_usd_output_custom_path or ""),
    }


def selected_jobs_editable(
    selected_jobs: list[RenderJob],
    *,
    can_edit_job: Callable[[RenderJob], bool],
) -> bool:
    return all(can_edit_job(job) for job in selected_jobs)


def should_show_custom_devices(*, mixed_device_mode: bool, first_device_mode: str) -> bool:
    current_device_mode = DeviceOverrideMode.coerce(first_device_mode)
    return current_device_mode is DeviceOverrideMode.SPECIFIC_GPUS and not mixed_device_mode


def has_active_or_locked_jobs(
    selected_jobs: list[RenderJob],
    *,
    is_active_job: Callable[[RenderJob], bool],
    is_locked_job: Callable[[RenderJob], bool],
) -> bool:
    return any(is_active_job(job) or is_locked_job(job) for job in selected_jobs)
