from __future__ import annotations

import re
from pathlib import Path

from queue_models import UsdOutputDirectoryMode


def job_file_name_from_path(hip_path: str) -> str:
    path = str(hip_path or "").strip()
    if not path:
        return "-"
    return Path(path).name or path


def job_rop_name_from_path(rop_path: str) -> str:
    value = str(rop_path or "").strip().rstrip("/")
    if not value:
        return "-"
    return value.split("/")[-1] or value


def safe_usd_folder_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name or "").strip()).strip("_")
    return cleaned or "rop"


def configured_retained_usd_folder_preview(
    *,
    hip_path: str,
    rop_path: str,
    mode: UsdOutputDirectoryMode,
    custom_path: str,
) -> str:
    if mode is UsdOutputDirectoryMode.DEFAULT_TEMP:
        return ""
    if mode is UsdOutputDirectoryMode.PROJECT_PATH:
        path = str(hip_path or "").strip()
        if not path:
            return ""
        hip_name = Path(path).stem or "hip"
        base_dir = Path(path).parent / "usd_renders" / safe_usd_folder_name(hip_name)
    else:
        value = str(custom_path or "").strip()
        if not value:
            return ""
        base_dir = Path(value)
    rop_name = safe_usd_folder_name(job_rop_name_from_path(rop_path))
    return str(base_dir / f"{rop_name}_$RENDERID")
