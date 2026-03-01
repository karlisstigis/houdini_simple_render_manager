from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from uuid import uuid4


class JobStatus(str, Enum):
    QUEUED = "Queued"
    RUNNING = "Running"
    INTERRUPTED = "Interrupted"
    DONE = "Done"
    FAILED = "Failed"
    CANCELED = "Canceled"
    OFFLINE = "Offline"


class FrameHandlingMode(str, Enum):
    OVERWRITE = "overwrite"
    RENDER_MISSING = "render_missing"
    RENDER_FROM_FIRST_MISSING = "render_from_first_missing"

    @classmethod
    def coerce(cls, value: str | None) -> "FrameHandlingMode":
        txt = str(value or "").strip().lower()
        for mode in cls:
            if mode.value == txt:
                return mode
        return cls.RENDER_MISSING

    def label(self) -> str:
        if self is FrameHandlingMode.OVERWRITE:
            return "Overwrite"
        if self is FrameHandlingMode.RENDER_MISSING:
            return "Render Missing"
        return "Render From First Missing"

    @classmethod
    def from_label(cls, label: str | None) -> "FrameHandlingMode":
        txt = str(label or "").strip().lower()
        if txt == "overwrite":
            return cls.OVERWRITE
        if txt == "render missing":
            return cls.RENDER_MISSING
        if txt == "render from first missing":
            return cls.RENDER_FROM_FIRST_MISSING
        return cls.RENDER_MISSING


@dataclass
class RenderJobSpec:
    hip_path: str
    rop_path: str
    frame_range_mode: str
    start_frame: int | None = None
    end_frame: int | None = None
    step: int | None = None
    name: str = ""
    enabled: bool = True
    frame_handling_mode: FrameHandlingMode = FrameHandlingMode.RENDER_MISSING
    strict_frame_range: bool = False
    id: str = field(default_factory=lambda: uuid4().hex)


@dataclass
class RenderJobRuntime:
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    log_file_path: str = ""
    error_summary: str = ""
    interrupted_reason: str = ""
    offline_detected_reason: str = ""
    runtime_start_frame: float | None = None
    runtime_end_frame: float | None = None
    runtime_step: float | None = None
    allframesatonce_enabled: bool | None = None
    offline_previous_status: JobStatus | None = None
    resume_start_frame_runtime: int | None = None
    resume_end_frame_runtime: int | None = None
    resume_step_runtime: int | None = None
    resume_completed_baseline_count: int = 0
    chunk_start_frame_runtime: int | None = None
    chunk_end_frame_runtime: int | None = None
    chunk_step_runtime: int | None = None
    chunk_index_runtime: int = 0
    chunk_total_runtime: int = 0
    chunk_attempt_runtime: int = 0
    chunk_retry_count_runtime: int = 0
    chunk_ranges_runtime: list[tuple[int, int, int]] = field(default_factory=list)
    chunk_retry_total_failures_runtime: int = 0


@dataclass
class RenderJobView:
    progress_text: str = ""
    percent_text: str = ""
    usd_build_percent: int | None = None
    last_frame_seen: float | None = None
    build_pass_completed: bool = False
    phase_text: str = ""
    prev_frame_time_text: str = "-"
    avg_frame_time_text: str = "-"
    est_job_time_text: str = "-"
    out_path: str = ""
    out_file_sample_path: str = ""
    render_frame_started_at: dict[str, float] = field(default_factory=dict)
    render_frame_durations_sec: list[float] = field(default_factory=list)
    render_completed_frames: set[str] = field(default_factory=set)


_SPEC_FIELDS = {
    "hip_path",
    "rop_path",
    "frame_range_mode",
    "start_frame",
    "end_frame",
    "step",
    "name",
    "enabled",
    "frame_handling_mode",
    "strict_frame_range",
    "id",
}
_RUNTIME_FIELDS = {
    "status",
    "created_at",
    "started_at",
    "finished_at",
    "exit_code",
    "log_file_path",
    "error_summary",
    "interrupted_reason",
    "offline_detected_reason",
    "runtime_start_frame",
    "runtime_end_frame",
    "runtime_step",
    "allframesatonce_enabled",
    "offline_previous_status",
    "resume_start_frame_runtime",
    "resume_end_frame_runtime",
    "resume_step_runtime",
    "resume_completed_baseline_count",
    "chunk_start_frame_runtime",
    "chunk_end_frame_runtime",
    "chunk_step_runtime",
    "chunk_index_runtime",
    "chunk_total_runtime",
    "chunk_attempt_runtime",
    "chunk_retry_count_runtime",
    "chunk_ranges_runtime",
    "chunk_retry_total_failures_runtime",
}
_VIEW_FIELDS = {
    "progress_text",
    "percent_text",
    "usd_build_percent",
    "last_frame_seen",
    "build_pass_completed",
    "phase_text",
    "prev_frame_time_text",
    "avg_frame_time_text",
    "est_job_time_text",
    "out_path",
    "out_file_sample_path",
    "render_frame_started_at",
    "render_frame_durations_sec",
    "render_completed_frames",
}


class RenderJob:
    def __init__(
        self,
        hip_path: str,
        rop_path: str,
        frame_range_mode: str,
        start_frame: int | None = None,
        end_frame: int | None = None,
        step: int | None = None,
        name: str = "",
        status: JobStatus = JobStatus.QUEUED,
        enabled: bool = True,
        frame_handling_mode: FrameHandlingMode = FrameHandlingMode.RENDER_MISSING,
        created_at: datetime | None = None,
        id: str | None = None,
    ) -> None:
        object.__setattr__(
            self,
            "spec",
            RenderJobSpec(
                hip_path=hip_path,
                rop_path=rop_path,
                frame_range_mode=frame_range_mode,
                start_frame=start_frame,
                end_frame=end_frame,
                step=step,
                name=name,
                enabled=enabled,
                frame_handling_mode=frame_handling_mode,
                id=str(id or uuid4().hex),
            ),
        )
        object.__setattr__(
            self,
            "runtime",
            RenderJobRuntime(
                status=status,
                created_at=created_at or datetime.now(),
            ),
        )
        object.__setattr__(self, "view", RenderJobView())

    def __getattr__(self, name: str):
        if name in _SPEC_FIELDS:
            return getattr(self.spec, name)
        if name in _RUNTIME_FIELDS:
            return getattr(self.runtime, name)
        if name in _VIEW_FIELDS:
            return getattr(self.view, name)
        raise AttributeError(name)

    def __setattr__(self, name: str, value) -> None:
        if name in {"spec", "runtime", "view"}:
            object.__setattr__(self, name, value)
            return
        if name in _SPEC_FIELDS:
            setattr(self.spec, name, value)
            return
        if name in _RUNTIME_FIELDS:
            setattr(self.runtime, name, value)
            return
        if name in _VIEW_FIELDS:
            setattr(self.view, name, value)
            return
        object.__setattr__(self, name, value)

    def display_name(self) -> str:
        if self.name.strip():
            return self.name.strip()
        hip_name = Path(self.hip_path).stem or "job"
        rop_name = self.rop_path.rstrip("/").split("/")[-1] if self.rop_path else "rop"
        return f"{hip_name} | {rop_name}"

    def frame_display(self) -> str:
        if self.frame_range_mode == "override":
            return f"{self.start_frame}-{self.end_frame}x{self.step}"
        return "Use ROP"

    def frame_range_display(self) -> str:
        if self.frame_range_mode == "override":
            return f"{self.start_frame}-{self.end_frame}"
        if self.runtime_start_frame is not None and self.runtime_end_frame is not None:
            start = int(self.runtime_start_frame) if float(self.runtime_start_frame).is_integer() else self.runtime_start_frame
            end = int(self.runtime_end_frame) if float(self.runtime_end_frame).is_integer() else self.runtime_end_frame
            return f"{start}-{end}"
        return "From ROP"

    def step_display(self) -> str:
        if self.frame_range_mode == "override":
            return str(self.step if self.step is not None else "-")
        if self.runtime_step is not None:
            step = int(self.runtime_step) if float(self.runtime_step).is_integer() else self.runtime_step
            return str(step)
        return "From ROP"

    def total_override_frames(self) -> int | None:
        if self.frame_range_mode != "override":
            return None
        if self.start_frame is None or self.end_frame is None or self.step is None or self.step <= 0:
            return None
        if self.end_frame < self.start_frame:
            return 0
        return ((self.end_frame - self.start_frame) // self.step) + 1

    def frame_handling_label(self) -> str:
        return self.frame_handling_mode.label()
