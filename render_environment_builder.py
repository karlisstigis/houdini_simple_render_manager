from __future__ import annotations

from pathlib import Path

from queue_models import DeviceOverrideMode


def parse_device_selection(selection: str) -> tuple[bool, list[str]]:
    selected_tokens = [part.strip().lower() for part in str(selection or "").split(",") if part.strip()]
    cpu_selected = "cpu" in selected_tokens
    selected_gpu_ids = [part for part in selected_tokens if part.isdigit()]
    return cpu_selected, selected_gpu_ids


def available_gpu_ids(devices: list[dict[str, str]]) -> list[str]:
    return [str(device.get("id", "") or "") for device in devices if str(device.get("id", "") or "").isdigit()]


def base_render_environment(
    *,
    mode: DeviceOverrideMode,
    selection: str,
    cpu_selected: bool,
    single_process_render: bool,
    retain_usd_enabled: bool,
    retained_usd_helper_path: Path,
) -> dict[str, str]:
    return {
        "HSRM_DEVICE_MODE": mode.value,
        "HSRM_DEVICE_SELECTION": selection,
        "HSRM_DEVICE_INCLUDE_CPU": "1" if cpu_selected else "0",
        "HSRM_RENDER_ALL_FRAMES_SINGLE_PROCESS": "1" if single_process_render else "0",
        "HSRM_RETAIN_USD_ENABLED": "1" if retain_usd_enabled else "0",
        "HSRM_RETAIN_USD_HELPER_PATH": str(retained_usd_helper_path),
    }


def should_delete_existing_retained_usd(
    *,
    output_path: str,
    reuse_retained_usd: bool,
    invalid_reason: str,
) -> bool:
    return bool(output_path and ((not reuse_retained_usd) or bool(invalid_reason)))


def should_reuse_existing_usd(
    *,
    reuse_retained_usd: bool,
    output_path: str,
    retained_reusable: bool,
    invalid_reason: str,
) -> bool:
    return bool(
        reuse_retained_usd
        and output_path
        and retained_reusable
        and Path(output_path).exists()
        and not invalid_reason
    )


def apply_retained_usd_env(
    env: dict[str, str],
    *,
    output_path: str,
    configured_output_dir: str,
    reuse_existing: bool,
) -> None:
    if reuse_existing and output_path:
        env["HSRM_RETAIN_USD_OUTPUT_PATH"] = output_path
    elif configured_output_dir:
        env["HSRM_RETAIN_USD_OUTPUT_DIR"] = configured_output_dir
    env["HSRM_REUSE_EXISTING_USD"] = "1" if reuse_existing else "0"


def apply_device_env(
    env: dict[str, str],
    *,
    mode: DeviceOverrideMode,
    all_gpu_ids: list[str],
    selected_gpu_ids: list[str],
    cpu_selected: bool,
) -> None:
    if mode is DeviceOverrideMode.CPU:
        env["HOUDINI_OCL_DEVICETYPE"] = "CPU"
        env["CUDA_VISIBLE_DEVICES"] = "-1"
    elif mode is DeviceOverrideMode.ALL_GPUS:
        env["HOUDINI_OCL_DEVICETYPE"] = "GPU"
        if all_gpu_ids:
            env["CUDA_VISIBLE_DEVICES"] = ",".join(all_gpu_ids)
    elif mode is DeviceOverrideMode.SPECIFIC_GPUS:
        if selected_gpu_ids:
            env["HOUDINI_OCL_DEVICETYPE"] = "GPU"
            env["CUDA_VISIBLE_DEVICES"] = ",".join(selected_gpu_ids)
        elif cpu_selected:
            env["HOUDINI_OCL_DEVICETYPE"] = "CPU"
            env["CUDA_VISIBLE_DEVICES"] = "-1"
