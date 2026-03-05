from __future__ import annotations

from pathlib import Path
from typing import Callable


def resolve_job_preview_path(
    *,
    candidate: str,
    resolved_range: tuple[int, int, int] | None,
    frame_path_for_frame: Callable[[str, int], Path | None],
) -> Path | None:
    raw_candidate = str(candidate or "").strip()
    if not raw_candidate or raw_candidate.lower() == "ip":
        return None

    direct_path = Path(raw_candidate)
    if direct_path.exists() and direct_path.is_file():
        return direct_path

    if resolved_range is not None:
        start_frame, end_frame, step = resolved_range
        for frame in range(start_frame, end_frame + 1, step):
            seq_path = frame_path_for_frame(raw_candidate, frame)
            if seq_path is not None and seq_path.exists() and seq_path.is_file():
                return seq_path
        fallback_seq = frame_path_for_frame(raw_candidate, start_frame)
        if fallback_seq is not None:
            return fallback_seq

    return direct_path if direct_path.suffix else None
