from __future__ import annotations

from pathlib import Path
from typing import Callable


def first_missing_frame_and_contiguous_done(
    *,
    start_frame: int,
    end_frame: int,
    step: int,
    path_for_frame: Callable[[int], Path | None],
    exists_nonempty: Callable[[Path], bool],
) -> tuple[int | None, int, int] | None:
    total = ((end_frame - start_frame) // step) + 1
    contiguous_done = 0
    for frame in range(start_frame, end_frame + 1, step):
        expected = path_for_frame(frame)
        if expected is None:
            return None
        if exists_nonempty(expected):
            contiguous_done += 1
            continue
        return frame, contiguous_done, total
    return None, contiguous_done, total


def compress_missing_frames_to_runs(missing_frames: list[int], *, step: int) -> list[tuple[int, int, int]]:
    if not missing_frames:
        return []
    runs: list[tuple[int, int, int]] = []
    run_start = missing_frames[0]
    run_prev = missing_frames[0]
    for frame in missing_frames[1:]:
        if frame == run_prev + step:
            run_prev = frame
            continue
        runs.append((run_start, run_prev, step))
        run_start = frame
        run_prev = frame
    runs.append((run_start, run_prev, step))
    return runs


def missing_frame_runs_and_existing_count(
    *,
    start_frame: int,
    end_frame: int,
    step: int,
    path_for_frame: Callable[[int], Path | None],
    exists_nonempty: Callable[[Path], bool],
) -> tuple[list[tuple[int, int, int]], int] | None:
    existing_count = 0
    missing_frames: list[int] = []
    for frame in range(start_frame, end_frame + 1, step):
        expected = path_for_frame(frame)
        if expected is None:
            return None
        if exists_nonempty(expected):
            existing_count += 1
        else:
            missing_frames.append(frame)
    return compress_missing_frames_to_runs(missing_frames, step=step), existing_count
