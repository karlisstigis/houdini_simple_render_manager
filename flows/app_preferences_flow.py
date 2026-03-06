from __future__ import annotations

from typing import Any

from queue_core.queue_models import DeviceOverrideMode, RenderJob, UsdOutputDirectoryMode
from ui_core.theme_support import normalize_theme_colors


def dialog_runtime_defaults(
    *,
    chunking_enabled: bool,
    chunk_size: int,
    retry_count: int,
    retry_delay: int,
) -> dict[str, Any]:
    return {
        "chunking_enabled": bool(chunking_enabled),
        "chunk_size": int(chunk_size),
        "retry_count": int(retry_count),
        "retry_delay": int(retry_delay),
    }


def dialog_experimental_flags(*, chunking_enabled: bool) -> dict[str, Any]:
    return {"chunking": bool(chunking_enabled)}


def dialog_startup_options(
    *,
    check_files_on_startup: bool,
    reload_all_jobs_on_startup: bool,
) -> dict[str, Any]:
    return {
        "check_files_on_startup": bool(check_files_on_startup),
        "reload_all_jobs_on_startup": bool(reload_all_jobs_on_startup),
    }


def dialog_device_defaults(
    *,
    mode: DeviceOverrideMode,
    selection: str,
    retain_built_usd: bool,
    usd_output_directory_mode: UsdOutputDirectoryMode,
    usd_output_directory_custom_path: str,
) -> dict[str, Any]:
    return {
        "mode": mode.value,
        "selection": selection,
        "retain_built_usd": bool(retain_built_usd),
        "usd_output_directory_mode": usd_output_directory_mode.value,
        "usd_output_directory_custom_path": str(usd_output_directory_custom_path or ""),
    }


def parse_runtime_defaults(raw: Any) -> tuple[bool, int, int, int] | None:
    if not isinstance(raw, dict):
        return None
    try:
        chunking_enabled = bool(raw.get("chunking_enabled", False))
        chunk_size = max(1, int(raw.get("chunk_size", 10)))
        retry_count = max(0, int(raw.get("retry_count", 1)))
        retry_delay = max(0, int(raw.get("retry_delay", 5)))
    except (TypeError, ValueError):
        chunking_enabled = False
        chunk_size = 10
        retry_count = 1
        retry_delay = 5
    return chunking_enabled, chunk_size, retry_count, retry_delay


def parse_device_defaults(raw: Any) -> tuple[DeviceOverrideMode, str, bool, UsdOutputDirectoryMode, str] | None:
    if not isinstance(raw, dict):
        return None
    mode = DeviceOverrideMode.coerce(raw.get("mode"))
    selection = RenderJob.normalize_device_selection(raw.get("selection", ""))
    retain_built_usd = bool(raw.get("retain_built_usd", False))
    usd_output_directory_mode = UsdOutputDirectoryMode.coerce(raw.get("usd_output_directory_mode"))
    usd_output_directory_custom_path = str(raw.get("usd_output_directory_custom_path", "") or "").strip()
    return mode, selection, retain_built_usd, usd_output_directory_mode, usd_output_directory_custom_path


def parse_startup_options(raw: Any) -> tuple[bool, bool] | None:
    if not isinstance(raw, dict):
        return None
    return (
        bool(raw.get("check_files_on_startup", True)),
        bool(raw.get("reload_all_jobs_on_startup", True)),
    )


def parse_preferences_payload(payload: dict[str, Any]) -> dict[str, Any]:
    hbatch_path = str(payload.get("hbatch_path", "") or "").strip()
    player_path = str(payload.get("player_path", "") or "").strip()
    theme = payload.get("theme", {})
    runtime_defaults = parse_runtime_defaults(payload.get("runtime_defaults", {}))
    device_defaults = parse_device_defaults(payload.get("device_defaults", {}))
    startup_options = parse_startup_options(payload.get("startup_options", {}))
    experimental_flags = payload.get("experimental_flags", {})
    return {
        "hbatch_path": hbatch_path,
        "player_path": player_path,
        "theme": normalize_theme_colors(theme) if isinstance(theme, dict) else None,
        "runtime_defaults": runtime_defaults,
        "device_defaults": device_defaults,
        "startup_options": startup_options,
        "experimental_chunking_enabled": bool(experimental_flags.get("chunking", False))
        if isinstance(experimental_flags, dict)
        else None,
    }
