from __future__ import annotations

import re
from dataclasses import dataclass

from queue_models import JobStatus, RenderJob


@dataclass
class RecoveryNotice:
    job_id: str
    title: str
    message: str
    technical_message: str
    severity: str


@dataclass
class RecoverySummary:
    recovered_count: int
    headline: str
    notices: list[RecoveryNotice]


def _headline_for_count(count: int) -> str:
    if count == 1:
        return "Recovered 1 interrupted job from the queue file."
    return f"Recovered {count} interrupted jobs from the queue file."


def _normalize_reason(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return "The previous render did not finish cleanly."
    text = re.sub(r"\s+", " ", text).strip()
    low = text.lower()
    if "app closed or crashed while this job was active" in low:
        return "The previous render session ended unexpectedly."
    if "recovered from a stale running state" in low:
        return "The previous render did not finish cleanly."
    if "became unresponsive" in low:
        text = re.sub(r"render worker became unresponsive\.?\s*", "", text, flags=re.IGNORECASE).strip()
        if text.lower().startswith("last active:"):
            chunk_text = text[len("last active:") :].strip().rstrip(".")
            return f"The previous render became unresponsive. Last active {chunk_text}."
        return "The previous render became unresponsive."
    if low.startswith("probe_failed:"):
        return "The previous render session ended with a recovery error."
    return text.rstrip(".") + "."


def build_startup_recovery_summary(jobs: list[RenderJob]) -> RecoverySummary | None:
    notices: list[RecoveryNotice] = []
    for job in jobs:
        if job.runtime.status != JobStatus.INTERRUPTED:
            continue
        reason = _normalize_reason(job.runtime.interrupted_reason)
        display_name = job.display_name()
        notices.append(
            RecoveryNotice(
                job_id=job.id,
                title=display_name,
                message=f"Recovered: {display_name}. {reason}",
                technical_message=f"Recovered interrupted job: {display_name} | reason={job.runtime.interrupted_reason or 'n/a'}",
                severity="warning",
            )
        )
    if not notices:
        return None
    return RecoverySummary(
        recovered_count=len(notices),
        headline=_headline_for_count(len(notices)),
        notices=notices,
    )
