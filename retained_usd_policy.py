from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def retained_usd_metadata_path(retained_usd_path: Path) -> Path:
    return retained_usd_path.with_name(f"{retained_usd_path.name}.hsrm.json")


def retained_usd_build_info(metadata: dict[str, Any] | None) -> tuple[str, str]:
    if not isinstance(metadata, dict):
        return "-", "-"
    start = metadata.get("start_frame")
    end = metadata.get("end_frame")
    step = metadata.get("step")
    if start is None or end is None:
        range_text = "-"
    else:
        range_text = f"{int(start)}-{int(end)}"
    if step is None:
        step_text = "-"
    else:
        step_text = str(int(step))
    return range_text, step_text


def retained_usd_built_at_text(metadata: dict[str, Any] | None) -> str:
    if not isinstance(metadata, dict):
        return "-"
    raw = str(metadata.get("built_at", "") or "").strip()
    if not raw:
        return "-"
    try:
        return datetime.fromisoformat(raw).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw


def retained_usd_hip_stale_reason(hip_path: str, metadata: dict[str, Any] | None) -> str:
    if not isinstance(metadata, dict):
        return ""
    normalized_hip_path = str(hip_path or "").strip()
    if not normalized_hip_path:
        return ""
    built_hip_mtime = metadata.get("hip_mtime")
    if built_hip_mtime is None:
        return ""
    try:
        current_hip_mtime = Path(normalized_hip_path).stat().st_mtime
    except OSError:
        return ""
    try:
        built_hip_mtime_value = float(built_hip_mtime)
    except (TypeError, ValueError):
        return ""
    if current_hip_mtime > built_hip_mtime_value:
        return "Potentially stale: HIP was saved after this USD was built."
    return ""


def retained_usd_invalid_reason(
    *,
    single_process_render_enabled: bool,
    retain_built_usd: bool,
    reuse_retained_usd: bool,
    retained_path: str,
    metadata: dict[str, Any] | None,
    current_range: tuple[int, int, int] | None,
) -> str:
    if not single_process_render_enabled:
        if retain_built_usd or reuse_retained_usd:
            return "Cannot retain or reuse USD: enable Render All Frames with a Single Process."
        return ""
    if not retained_path:
        return ""
    if not isinstance(metadata, dict):
        return "Cannot reuse USD: build metadata is unavailable."
    if current_range is None:
        return ""

    current_start, current_end, current_step = map(int, current_range)
    try:
        built_start = int(metadata.get("start_frame", current_start))
        built_end = int(metadata.get("end_frame", current_end))
        built_step = int(metadata.get("step", current_step))
    except (TypeError, ValueError):
        return "Cannot reuse USD: build metadata is unavailable."

    if current_start < built_start or current_end > built_end:
        return "Cannot reuse USD: current frame range exceeds the built USD range."
    if built_step != 1:
        if current_step == built_step:
            if ((current_start - built_start) % built_step) == 0:
                return ""
            return "Cannot reuse USD: current frame range is not aligned with the built USD step."
        return "Cannot reuse USD: retained USD must be built with step 1 to reuse with a different step."
    return ""


def retained_usd_status_text(
    *,
    single_process_render_enabled: bool,
    retain_built_usd: bool,
    reuse_retained_usd: bool,
    retained_path: str,
    retained_usd_exists: bool,
    metadata: dict[str, Any] | None,
    invalid_reason: str,
) -> str:
    if not single_process_render_enabled and (retain_built_usd or reuse_retained_usd):
        return "Requires single-process render"
    if not retain_built_usd:
        return "Build on next render"
    if not retained_path or not retained_usd_exists:
        return "Build on next render"
    if not reuse_retained_usd:
        return "Build on next render"
    if not isinstance(metadata, dict):
        return "Build on next render"
    if invalid_reason:
        return "Build on next render"
    return "Reuse on next render"
