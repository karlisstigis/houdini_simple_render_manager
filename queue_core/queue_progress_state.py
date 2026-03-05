from __future__ import annotations

import re

from queue_core.queue_models import JobStatus, RenderJob


def job_phase_display(job: RenderJob) -> str:
    phase = (job.view.phase_text or "").strip()
    if job.runtime.chunk_total_runtime > 1:
        chunk_part = f"Chunk {max(1, job.runtime.chunk_index_runtime + 1)}/{job.runtime.chunk_total_runtime}"
        if job.runtime.chunk_attempt_runtime > 1:
            chunk_part += f" r{job.runtime.chunk_attempt_runtime}"
        if phase:
            return f"{phase} ({chunk_part})"
        return chunk_part
    return phase


def parse_percent_value(text: str) -> int | None:
    match = re.search(r"(\d{1,3})\s*%", str(text or ""))
    if not match:
        return None
    try:
        return max(0, min(100, int(match.group(1))))
    except ValueError:
        return None


def queue_progress_split_values(job: RenderJob) -> tuple[int | None, int | None]:
    pct = parse_percent_value(job.view.percent_text)
    build_pct: int | None = None
    render_pct: int | None = None
    show_usd_build = job.runtime.allframesatonce_enabled is True

    if job.runtime.status == JobStatus.DONE:
        render_pct = 100
        if show_usd_build and job.view.usd_build_percent is not None:
            build_pct = job.view.usd_build_percent
        else:
            build_pct = 100 if show_usd_build else None
        return build_pct, render_pct

    if show_usd_build and job.view.phase_text == "USD Build":
        build_pct = job.view.usd_build_percent if job.view.usd_build_percent is not None else pct
        render_pct = 0
        return build_pct, render_pct
    if job.view.phase_text == "Render":
        render_pct = pct
        if show_usd_build and job.view.usd_build_percent is not None:
            build_pct = job.view.usd_build_percent
        else:
            build_pct = 100 if show_usd_build and job.view.build_pass_completed else None
        return build_pct, render_pct

    if pct is not None:
        render_pct = pct
    if show_usd_build and job.view.usd_build_percent is not None:
        build_pct = job.view.usd_build_percent
    return build_pct, render_pct
