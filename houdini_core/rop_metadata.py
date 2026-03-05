"""Typed ROP metadata records plus shared parse/apply helpers for scan/probe flows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class RopInfo:
    error: str | None = None
    strict_frame_range: bool | None = None
    all_frames_single_process: bool | None = None
    runtime_start_frame: float | None = None
    runtime_end_frame: float | None = None
    runtime_step: float | None = None
    output_path: str = ""
    returncode: int | None = None
    combined_output: str = ""

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def new_rop_info_record() -> RopInfo:
    return RopInfo()


def parse_probe_rop_info_output(combined: str, returncode: int | None) -> RopInfo:
    info = new_rop_info_record()
    info.returncode = returncode
    info.combined_output = combined

    strict_match = re.search(r"__HSRM_TRANGE_STRICT__\|([01])", combined)
    if strict_match:
        info.strict_frame_range = strict_match.group(1) == "1"
    allframes_match = re.search(r"__HSRM_ALLFRAMESATONCE__\|([01])", combined)
    if allframes_match:
        info.all_frames_single_process = allframes_match.group(1) == "1"

    range_match = re.search(
        r"__HSRM_RANGE__\|(-?\d+(?:\.\d+)?)\|(-?\d+(?:\.\d+)?)\|(-?\d+(?:\.\d+)?)",
        combined,
    )
    if range_match:
        try:
            info.runtime_start_frame = float(range_match.group(1))
            info.runtime_end_frame = float(range_match.group(2))
            info.runtime_step = float(range_match.group(3))
        except ValueError:
            pass

    out_match = re.search(r"__HSRM_OUT__\|([^\r\n]+)", combined)
    if out_match:
        info.output_path = out_match.group(1).strip()

    err_match = re.search(r"__HSRM_RANGE_ERR__\|([^\r\n]+)", combined)
    if err_match:
        info.error = err_match.group(1)
    return info


def rop_info_from_scan_record(record: dict[str, Any]) -> RopInfo:
    info = new_rop_info_record()
    info.strict_frame_range = bool(record.get("strict_frame_range"))
    all_frames_value = record.get("all_frames_single_process")
    info.all_frames_single_process = None if all_frames_value is None else bool(all_frames_value)
    info.output_path = str(record.get("output_path", "") or "").strip()
    info.runtime_start_frame = record.get("runtime_start_frame")
    info.runtime_end_frame = record.get("runtime_end_frame")
    info.runtime_step = record.get("runtime_step")
    return info


def apply_rop_info_to_job(
    job: Any,
    info: RopInfo,
    normalize_output_display_path: Callable[[str], str],
    *,
    apply_runtime_range: bool = True,
    apply_single_process_setting: bool = False,
) -> None:
    if info.strict_frame_range is not None:
        job.strict_frame_range = bool(info.strict_frame_range)
    if apply_single_process_setting and info.all_frames_single_process is not None:
        job.render_all_frames_single_process = bool(info.all_frames_single_process)

    out_probe = str(info.output_path or "").strip()
    if out_probe:
        job.out_file_sample_path = out_probe
        job.out_path = normalize_output_display_path(out_probe)

    if not apply_runtime_range:
        return
    if info.runtime_start_frame is not None and info.runtime_end_frame is not None:
        job.runtime_start_frame = info.runtime_start_frame
        job.runtime_end_frame = info.runtime_end_frame
        job.runtime_step = info.runtime_step
        job.runtime.rop_default_start_frame = info.runtime_start_frame
        job.runtime.rop_default_end_frame = info.runtime_end_frame
        job.runtime.rop_default_step = info.runtime_step
