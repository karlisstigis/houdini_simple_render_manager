from __future__ import annotations

from typing import Callable

from queue_models import DeviceOverrideMode, RenderJob, UsdOutputDirectoryMode


JobPropertyApplyFn = Callable[[RenderJob], bool]
JobPropertyEditSpec = tuple[str, JobPropertyApplyFn]


def device_mode_edit_spec(value: str) -> JobPropertyEditSpec:
    mode = DeviceOverrideMode.coerce(value)

    def _apply(job: RenderJob) -> bool:
        if job.spec.device_override_mode is mode:
            return False
        job.spec.device_override_mode = mode
        return True

    return "device_override_mode", _apply


def device_selection_edit_spec(value: str) -> JobPropertyEditSpec:
    normalized = RenderJob.normalize_device_selection(value)

    def _apply(job: RenderJob) -> bool:
        if job.spec.device_selection == normalized:
            return False
        job.spec.device_selection = normalized
        return True

    return "device_selection", _apply


def retain_built_usd_edit_spec(checked: bool) -> JobPropertyEditSpec:
    checked_bool = bool(checked)

    def _apply(job: RenderJob) -> bool:
        if bool(job.spec.retain_built_usd) == checked_bool and (checked_bool or not bool(job.spec.reuse_retained_usd)):
            return False
        job.spec.retain_built_usd = checked_bool
        if not checked_bool:
            job.spec.reuse_retained_usd = False
        return True

    return "retain_built_usd", _apply


def single_process_render_edit_spec(checked: bool) -> JobPropertyEditSpec:
    checked_bool = bool(checked)

    def _apply(job: RenderJob) -> bool:
        if bool(job.spec.render_all_frames_single_process) == checked_bool:
            return False
        job.spec.render_all_frames_single_process = checked_bool
        return True

    return "render_all_frames_single_process", _apply


def reuse_retained_usd_edit_spec(checked: bool) -> JobPropertyEditSpec:
    checked_bool = bool(checked)

    def _apply(job: RenderJob) -> bool:
        if bool(job.spec.reuse_retained_usd) == checked_bool:
            return False
        job.spec.reuse_retained_usd = checked_bool
        return True

    return "reuse_retained_usd", _apply


def usd_output_directory_mode_edit_spec(value: str) -> JobPropertyEditSpec:
    mode = UsdOutputDirectoryMode.coerce(value)

    def _apply(job: RenderJob) -> bool:
        if job.spec.usd_output_directory_mode is mode:
            return False
        job.spec.usd_output_directory_mode = mode
        return True

    return "usd_output_directory_mode", _apply


def usd_output_directory_custom_path_edit_spec(value: str) -> JobPropertyEditSpec:
    normalized = str(value or "").strip()

    def _apply(job: RenderJob) -> bool:
        if str(job.spec.usd_output_directory_custom_path or "") == normalized:
            return False
        job.spec.usd_output_directory_custom_path = normalized
        return True

    return "usd_output_directory_custom_path", _apply
