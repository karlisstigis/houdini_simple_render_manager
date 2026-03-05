from __future__ import annotations

from pathlib import Path
from typing import Callable


def initial_probe_path(sample_file_path: str, out_path: str) -> str:
    sample = str(sample_file_path or "").strip()
    out = str(out_path or "").strip()
    return sample or out


def needs_pattern_refresh(
    *,
    probe_path: str,
    sample_file_path: str,
    start_frame: int,
    frame_path_for_frame: Callable[[str, int], Path | None],
) -> bool:
    probe = str(probe_path or "").strip()
    sample = str(sample_file_path or "").strip()
    return (
        (not probe)
        or (not sample)
        or probe.lower() == "ip"
        or frame_path_for_frame(sample or probe, int(start_frame)) is None
    )


def path_exists_nonempty(path: Path) -> bool:
    try:
        exists = path.exists()
        return bool(exists and path.stat().st_size > 0)
    except OSError:
        return False
