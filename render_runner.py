"""Render command planning helpers for hbatch payload generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class RenderCommandPlan:
    command_text: str
    command_mode: str
    effective_start: int | None = None
    effective_end: int | None = None
    effective_step: int | None = None
    is_resume_runtime: bool = False


def build_render_command_plan(job: Any, quote_hscript: Callable[[str], str]) -> RenderCommandPlan:
    cmd = "render -V"
    command_mode = str(getattr(job, "frame_range_mode", "use_rop"))
    effective_start: int | None = None
    effective_end: int | None = None
    effective_step: int | None = None

    chunk_start = getattr(job, "chunk_start_frame_runtime", None)
    chunk_end = getattr(job, "chunk_end_frame_runtime", None)
    chunk_step = getattr(job, "chunk_step_runtime", None)
    resume_start = getattr(job, "resume_start_frame_runtime", None)
    resume_end = getattr(job, "resume_end_frame_runtime", None)
    resume_step = getattr(job, "resume_step_runtime", None)
    if chunk_start is not None and chunk_end is not None and chunk_step not in (None, 0):
        command_mode = f"{command_mode}+chunk"
        effective_start = int(chunk_start)
        effective_end = int(chunk_end)
        effective_step = int(chunk_step)
    elif resume_start is not None and resume_end is not None and resume_step not in (None, 0):
        command_mode = f"{command_mode}+resume"
        effective_start = int(resume_start)
        effective_end = int(resume_end)
        effective_step = int(resume_step)
    elif getattr(job, "frame_range_mode", "") == "override":
        start_frame = getattr(job, "start_frame", None)
        end_frame = getattr(job, "end_frame", None)
        if start_frame is None or end_frame is None:
            raise ValueError("Override frame range is missing start/end values.")
        effective_start = int(start_frame)
        effective_end = int(end_frame)
        effective_step = int(getattr(job, "step", 1) or 1)

    if effective_start is not None and effective_end is not None:
        cmd += f" -f {effective_start} {effective_end}"
        if effective_step and effective_step != 1:
            cmd += f" -i {effective_step}"

    rop_path = str(getattr(job, "rop_path", "") or "")
    cmd += f" {quote_hscript(rop_path)}"

    return RenderCommandPlan(
        command_text=cmd,
        command_mode=command_mode,
        effective_start=effective_start,
        effective_end=effective_end,
        effective_step=effective_step,
        is_resume_runtime=("+resume" in command_mode),
    )


def build_hbatch_command_payload(commands: list[str]) -> bytes:
    return ("\n".join(commands) + "\n").encode("utf-8", errors="replace")
