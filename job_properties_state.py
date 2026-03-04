from __future__ import annotations

from typing import Any


def default_job_properties_panel_state(
    *,
    default_usd_output_mode: str,
    default_usd_output_custom_path: str,
    retained_usd_defaults: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name_text": "-",
        "file_text": "-",
        "rop_text": "-",
        "editable": False,
        "device_mode": "default",
        "show_custom_devices": False,
        "device_options": [],
        "device_selection_enabled": False,
        "single_process_render_check_state": 0,
        "retain_check_state": 0,
        "reuse_check_state": 0,
        "reuse_enabled": False,
        "mixed_usd_output_mode": False,
        "usd_output_directory_mode": default_usd_output_mode,
        "usd_output_directory_custom_path": default_usd_output_custom_path,
        **dict(retained_usd_defaults),
    }


def tri_state_bool(
    *,
    mixed: bool,
    value: bool,
    unchecked: int,
    checked: int,
    partial: int,
) -> int:
    if mixed:
        return int(partial)
    return int(checked if value else unchecked)


def mixed_or_value_text(mixed: bool, value: Any, *, fallback: str = "-") -> str:
    if mixed:
        return "Mixed"
    return str(value or fallback)


def build_job_properties_panel_state(
    *,
    mixed_name: bool,
    first_name: Any,
    mixed_file: bool,
    first_file: Any,
    mixed_rop: bool,
    first_rop: Any,
    editable: bool,
    mixed_device_mode: bool,
    first_device_mode: str | None,
    show_custom_devices: bool,
    device_options: list[dict[str, Any]],
    mixed_single_process: bool,
    first_single_process: bool,
    mixed_retain: bool,
    first_retain: bool,
    mixed_reuse: bool,
    first_reuse: bool,
    mixed_usd_output_mode: bool,
    first_usd_output_mode: str | None,
    mixed_usd_output_custom_path: bool,
    first_usd_output_custom_path: str,
    retained_usd_state: dict[str, Any],
    can_delete: bool,
    unchecked_state: int,
    checked_state: int,
    partial_state: int,
    default_device_mode: str,
    default_usd_output_mode: str,
) -> dict[str, Any]:
    return {
        "name_text": mixed_or_value_text(mixed_name, first_name),
        "file_text": mixed_or_value_text(mixed_file, first_file),
        "rop_text": mixed_or_value_text(mixed_rop, first_rop),
        "editable": editable,
        "mixed_device_mode": mixed_device_mode,
        "device_mode": first_device_mode or default_device_mode,
        "show_custom_devices": show_custom_devices,
        "device_options": list(device_options),
        "device_selection_enabled": show_custom_devices,
        "single_process_render_check_state": tri_state_bool(
            mixed=mixed_single_process,
            value=bool(first_single_process),
            unchecked=unchecked_state,
            checked=checked_state,
            partial=partial_state,
        ),
        "retain_check_state": tri_state_bool(
            mixed=mixed_retain,
            value=bool(first_retain),
            unchecked=unchecked_state,
            checked=checked_state,
            partial=partial_state,
        ),
        "reuse_check_state": tri_state_bool(
            mixed=mixed_reuse,
            value=bool(first_reuse),
            unchecked=unchecked_state,
            checked=checked_state,
            partial=partial_state,
        ),
        "reuse_enabled": editable and bool(first_retain or mixed_retain),
        "mixed_usd_output_mode": mixed_usd_output_mode,
        "usd_output_directory_mode": first_usd_output_mode or default_usd_output_mode,
        "mixed_usd_output_custom_path": mixed_usd_output_custom_path,
        "usd_output_directory_custom_path": "" if mixed_usd_output_custom_path else str(first_usd_output_custom_path or ""),
        "retained_usd_path": retained_usd_state["retained_usd_path"],
        "retained_usd_built_range": retained_usd_state["retained_usd_built_range"],
        "retained_usd_built_step": retained_usd_state["retained_usd_built_step"],
        "retained_usd_built_at": retained_usd_state["retained_usd_built_at"],
        "retained_usd_status": retained_usd_state["retained_usd_status"],
        "retained_usd_warning": retained_usd_state["retained_usd_warning"],
        "can_open": bool(retained_usd_state["can_open"]),
        "can_delete": bool(can_delete),
    }
