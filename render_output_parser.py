"""Pure parsers for Houdini/husk render output markers and phase/runtime metadata."""

from __future__ import annotations

import re
import time
from typing import Any, Callable


def update_job_runtime_flags_from_output(
    job: Any,
    text: str,
    *,
    update_runtime_range: bool = True,
) -> None:
    if "[Preflight] allframesatonce=" in text:
        m = re.search(r"\[Preflight\]\s+allframesatonce=(\d+)", text)
        if m:
            job.allframesatonce_enabled = m.group(1) not in {"0"}

    if update_runtime_range:
        range_match = re.search(
            r"(?i)\brendering\s+\d+\s+frames?\s*\(\s*from\s+(-?\d+(?:\.\d+)?)\s+to\s+(-?\d+(?:\.\d+)?)\s+by\s+(-?\d+(?:\.\d+)?)\s*\)",
            text,
        )
        if range_match:
            try:
                job.runtime_start_frame = float(range_match.group(1))
                job.runtime_end_frame = float(range_match.group(2))
                job.runtime_step = float(range_match.group(3))
            except ValueError:
                pass


def detect_phase_from_output(text: str) -> str | None:
    if "__HSRM_PHASE__|Render|start" in text:
        return "Render"
    lower = text.lower()
    if "husk_mplay" in lower:
        lower = lower.replace("husk_mplay", "")

    if (
        "command: husk " in lower
        or ">>> render " in lower
        or re.search(r"\bhusk\s+version\b", lower)
        or re.search(r"\bhusk\b", lower)
        or "starting husk" in lower
        or "launching husk" in lower
        or "karma" in lower
        or "bucket" in lower
        or "samples/pixel" in lower
    ):
        return "Render"

    usd_build_markers = (
        "usd",
        "usda",
        "usdc",
        "writing layer",
        "writing usd",
        "building usd",
        "exporting usd",
        "saving usd",
        "flatten",
    )
    prep_markers = (
        "cook",
        "cooking",
        "generate",
        "generating",
        "build",
        "building",
        "write",
        "writing",
        "export",
        "exporting",
        "save",
        "saving",
        "stage",
    )
    if any(m in lower for m in usd_build_markers) and any(m in lower for m in prep_markers):
        return "USD Build"
    if "allframesatonce" in lower:
        return "USD Build"
    return None


def detect_phase_from_output_with_job(job: Any, text: str) -> str | None:
    phase = detect_phase_from_output(text)
    if phase:
        return phase

    lower = text.lower()
    if getattr(job, "allframesatonce_enabled", None) and getattr(job, "phase_text", "") != "Render":
        if re.search(r"(?i)\brendering\s+\d+\s+frames?\b", text):
            return "USD Build"
        if re.search(r"(?i)\bframe\s+-?\d+(?:\.\d+)?\b", text):
            return "USD Build"
        if "writing" in lower and "usd" in lower:
            return "USD Build"
    return None


def update_job_from_hsrm_markers(
    job: Any,
    text: str,
    normalize_output_display_path: Callable[[str], str],
    update_job_render_timing_stats_cb: Callable[[Any], None],
) -> None:
    for line in text.splitlines():
        if line.startswith("__HSRM_PHASE__|"):
            parts = line.strip().split("|")
            if len(parts) >= 3 and parts[1] == "Render":
                if parts[2] == "start":
                    job.phase_text = "Render"
                elif parts[2] == "end" and not job.phase_text:
                    job.phase_text = "Render"
        elif line.startswith("__HSRM_FRAME__|"):
            parts = line.strip().split("|")
            if len(parts) >= 3 and parts[1] in {"start", "end"}:
                frame_key = parts[2].strip()
                try:
                    frame_val = float(frame_key)
                except ValueError:
                    continue
                job.last_frame_seen = frame_val
                job.phase_text = "Render"
                if parts[1] == "start":
                    job.render_frame_started_at[frame_key] = time.monotonic()
                else:
                    job.render_completed_frames.add(frame_key)
                    started = job.render_frame_started_at.pop(frame_key, None)
                    if started is not None:
                        elapsed = max(0.0, time.monotonic() - started)
                        job.render_frame_durations_sec.append(elapsed)
                        update_job_render_timing_stats_cb(job)
        elif line.startswith("__HSRM_OUT__|"):
            out_path = line.split("|", 1)[1].strip()
            if out_path:
                job.out_file_sample_path = out_path
                job.out_path = normalize_output_display_path(out_path)
