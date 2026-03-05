"""Houdini-facing service helpers for script prep, scan, and probe flows."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Callable

from houdini_core.houdini_bridge import (
    REQUIRED_HOUDINI_SCRIPT_FILENAMES,
    build_render_preflight_script as build_render_preflight_script_bridge,
    ensure_husk_hook_files as ensure_husk_hook_files_bridge,
    ensure_runtime_script_copy,
    load_houdini_script_text as load_houdini_script_text_bridge,
    project_houdini_scripts_dir as project_houdini_scripts_dir_bridge,
    validate_houdini_script_files as validate_houdini_script_files_bridge,
)
from houdini_core.rop_metadata import (
    RopInfo,
    apply_rop_info_to_job as apply_rop_info_to_job_model,
    parse_probe_rop_info_output,
    rop_info_from_scan_record as rop_info_from_scan_record_model,
)


def project_houdini_scripts_dir(main_file: str, scripts_dir_name: str) -> Path:
    return project_houdini_scripts_dir_bridge(main_file, scripts_dir_name)


def required_houdini_script_filenames() -> list[str]:
    return list(REQUIRED_HOUDINI_SCRIPT_FILENAMES)


def validate_houdini_script_files(scripts_dir: Path) -> list[str]:
    return validate_houdini_script_files_bridge(scripts_dir)


def load_houdini_script_text(scripts_dir: Path, filename: str) -> str:
    return load_houdini_script_text_bridge(scripts_dir, filename)


def build_render_preflight_script(
    *,
    scripts_dir: Path,
    rop_path: str,
    disable_husk_mplay: bool,
    hook_paths: dict[str, str],
) -> str:
    return build_render_preflight_script_bridge(
        scripts_dir=scripts_dir,
        rop_path=rop_path,
        disable_husk_mplay=disable_husk_mplay,
        hook_paths=hook_paths,
    )


def ensure_husk_hook_files(
    *,
    scripts_dir: Path,
    hook_script_path_fn: Callable[[str], Path],
) -> dict[str, str]:
    return ensure_husk_hook_files_bridge(
        scripts_dir=scripts_dir,
        hook_script_path_fn=hook_script_path_fn,
    )


def ensure_range_probe_script(
    *,
    scripts_dir: Path,
    hook_script_path_fn: Callable[[str], Path],
) -> Path:
    return ensure_runtime_script_copy(
        scripts_dir=scripts_dir,
        source_filename="hsrm_resolve_range.py",
        target_stem="hsrm_resolve_range",
        hook_script_path_fn=hook_script_path_fn,
    )


def ensure_scan_script(
    *,
    scripts_dir: Path,
    hook_script_path_fn: Callable[[str], Path],
) -> Path:
    return ensure_runtime_script_copy(
        scripts_dir=scripts_dir,
        source_filename="hsrm_scan_nodes.py",
        target_stem="hsrm_scan_nodes_runtime",
        hook_script_path_fn=hook_script_path_fn,
    )


def parse_scan_output(stdout: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    in_block = False
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if line == "__HSRM_SCAN_BEGIN__":
            in_block = True
            continue
        if line == "__HSRM_SCAN_END__":
            in_block = False
            continue
        if not in_block or not line.startswith("__HSRM_NODE__|"):
            continue
        parts = line.split("|", 9)
        if len(parts) < 4:
            continue
        _, path, category, type_name = parts[:4]
        strict_flag = parts[4] if len(parts) >= 5 else "0"
        out_path = parts[5] if len(parts) >= 6 else ""
        rf1_text = parts[6] if len(parts) >= 7 else ""
        rf2_text = parts[7] if len(parts) >= 8 else ""
        rf3_text = parts[8] if len(parts) >= 9 else ""
        allframes_text = parts[9] if len(parts) >= 10 else ""
        if not (path.startswith("/out/") or path.startswith("/stage/")):
            continue
        try:
            rf1 = float(rf1_text) if str(rf1_text).strip() != "" else None
        except Exception:
            rf1 = None
        try:
            rf2 = float(rf2_text) if str(rf2_text).strip() != "" else None
        except Exception:
            rf2 = None
        try:
            rf3 = float(rf3_text) if str(rf3_text).strip() != "" else None
        except Exception:
            rf3 = None
        records.append(
            {
                "path": path,
                "category": category,
                "type_name": type_name,
                "strict_frame_range": strict_flag in {"1", "true", "True"},
                "output_path": out_path,
                "runtime_start_frame": rf1,
                "runtime_end_frame": rf2,
                "runtime_step": rf3,
                "all_frames_single_process": None if allframes_text == "" else (allframes_text in {"1", "true", "True"}),
            }
        )
    return records


def probe_rop_info(
    *,
    hbatch_path: str,
    hip_path: str,
    rop_path: str,
    probe_script_path: Path,
    hscript_quote: Callable[[str], str],
) -> RopInfo | None:
    py_path_arg = hscript_quote(str(probe_script_path))
    rop_arg = hscript_quote(rop_path)
    payload = "\n".join([f"python {py_path_arg} {rop_arg}", "quit"]) + "\n"
    try:
        result = subprocess.run(
            [hbatch_path, hip_path],
            input=payload,
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        return None
    combined = (result.stdout or "") + "\n" + (result.stderr or "")
    return parse_probe_rop_info_output(combined, result.returncode)


def probe_and_apply_job_rop_metadata(
    job: Any,
    *,
    hbatch_path: str,
    probe_script_path: Path,
    hscript_quote: Callable[[str], str],
    normalize_output_display_path: Callable[[str], str],
) -> tuple[RopInfo | None, str | None]:
    info = probe_rop_info(
        hbatch_path=hbatch_path,
        hip_path=job.hip_path,
        rop_path=job.rop_path,
        probe_script_path=probe_script_path,
        hscript_quote=hscript_quote,
    )
    if info is None:
        return None, None
    apply_rop_info_to_job_model(
        job,
        info,
        normalize_output_display_path,
        apply_runtime_range=True,
    )
    if info.runtime_start_frame is not None and info.runtime_end_frame is not None:
        return info, None
    if info.error:
        return info, str(info.error)
    return info, None


def probe_rop_strict_frame_range(
    *,
    hbatch_path: str,
    hip_path: str,
    rop_path: str,
    probe_script_path: Path,
    hscript_quote: Callable[[str], str],
) -> bool | None:
    info = probe_rop_info(
        hbatch_path=hbatch_path,
        hip_path=hip_path,
        rop_path=rop_path,
        probe_script_path=probe_script_path,
        hscript_quote=hscript_quote,
    )
    if info and info.strict_frame_range is not None:
        return bool(info.strict_frame_range)
    return None


def scan_rop_info_for_hip(
    *,
    hbatch_path: str,
    hip_path: str,
    scan_script_path: Path,
    roots: list[str],
    hscript_quote: Callable[[str], str],
) -> dict[str, RopInfo]:
    args = " ".join(hscript_quote(r) for r in roots)
    payload = f"python {hscript_quote(str(scan_script_path))} {args}\nquit\n"
    try:
        result = subprocess.run(
            [hbatch_path, hip_path],
            input=payload,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        return {}
    records = parse_scan_output((result.stdout or "") + "\n" + (result.stderr or ""))
    info_map: dict[str, RopInfo] = {}
    for rec in records:
        path = str(rec.get("path", "") or "").strip()
        if not path:
            continue
        info_map[path] = rop_info_from_scan_record_model(rec)
    return info_map
