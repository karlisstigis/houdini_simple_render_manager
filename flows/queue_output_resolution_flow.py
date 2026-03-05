from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def maybe_refresh_probe_path(
    *,
    probe_path: str,
    sample_file_path: str,
    start_frame: int,
    hip_exists: bool,
    hbatch_exists: bool,
    hip_path: str,
    rop_path: str,
    needs_pattern_refresh_fn: Callable[[str, str, int, Callable[[str, int], Path | None]], bool],
    frame_path_for_frame_fn: Callable[[str, int], Path | None],
    probe_rop_info_fn: Callable[[str, str], Any | None],
    apply_rop_info_fn: Callable[[Any], None],
    refreshed_sample_path_fn: Callable[[], str],
) -> tuple[str, bool]:
    needs_refresh = needs_pattern_refresh_fn(
        probe_path,
        sample_file_path,
        start_frame,
        frame_path_for_frame_fn,
    )
    if not (needs_refresh and hip_exists and hbatch_exists):
        return probe_path, False
    info = probe_rop_info_fn(hip_path, rop_path)
    if info is not None and str(getattr(info, "error", "") or "") == "node_not_found":
        return probe_path, True
    if info is not None:
        apply_rop_info_fn(info)
    refreshed_sample = str(refreshed_sample_path_fn() or "").strip()
    if refreshed_sample:
        return refreshed_sample, False
    return probe_path, False


def probe_pattern_resolved(
    *,
    probe_path: str,
    start_frame: int,
    frame_path_for_frame_fn: Callable[[str, int], Path | None],
) -> bool:
    return frame_path_for_frame_fn(probe_path, start_frame) is not None
