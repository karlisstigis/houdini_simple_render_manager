from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DiagnosticsSnapshot:
    app_name: str
    queue_path: str
    logs_dir: str
    hbatch_path: str
    player_path: str
    queue_active: bool
    queue_paused: bool
    current_job_id: str
    render_worker_active: bool
    scan_worker_active: bool
    render_worker_stderr: str
    scan_worker_stderr: str
    status_text: str
    recovery_headline: str


def build_diagnostics_report(snapshot: DiagnosticsSnapshot) -> str:
    lines = [
        f"App: {snapshot.app_name}",
        f"Queue File: {snapshot.queue_path or '(none)'}",
        f"Logs Dir: {snapshot.logs_dir or '(none)'}",
        f"hbatch Path: {snapshot.hbatch_path or '(none)'}",
        f"Preview Player: {snapshot.player_path or '(none)'}",
        f"Queue Active: {'yes' if snapshot.queue_active else 'no'}",
        f"Queue Paused: {'yes' if snapshot.queue_paused else 'no'}",
        f"Current Job ID: {snapshot.current_job_id or '(none)'}",
        f"Render Worker Active: {'yes' if snapshot.render_worker_active else 'no'}",
        f"Scan Worker Active: {'yes' if snapshot.scan_worker_active else 'no'}",
        f"Status Text: {snapshot.status_text or '(none)'}",
        f"Last Recovery Summary: {snapshot.recovery_headline or '(none)'}",
    ]
    render_stderr = (snapshot.render_worker_stderr or "").strip()
    scan_stderr = (snapshot.scan_worker_stderr or "").strip()
    if render_stderr:
        lines.append("")
        lines.append("Render Worker Stderr:")
        lines.append(render_stderr)
    if scan_stderr:
        lines.append("")
        lines.append("Scan Worker Stderr:")
        lines.append(scan_stderr)
    return "\n".join(lines) + "\n"
