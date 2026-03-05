from __future__ import annotations

from app_core.diagnostics import DiagnosticsSnapshot


def build_diagnostics_snapshot(
    *,
    app_name: str,
    queue_path: str,
    logs_dir: str,
    hbatch_path: str,
    player_path: str,
    queue_active: bool,
    queue_paused: bool,
    current_job_id: str,
    render_worker_active: bool,
    scan_worker_active: bool,
    render_worker_stderr: str,
    scan_worker_stderr: str,
    status_text: str,
    recovery_headline: str,
) -> DiagnosticsSnapshot:
    return DiagnosticsSnapshot(
        app_name=str(app_name or ""),
        queue_path=str(queue_path or ""),
        logs_dir=str(logs_dir or ""),
        hbatch_path=str(hbatch_path or ""),
        player_path=str(player_path or ""),
        queue_active=bool(queue_active),
        queue_paused=bool(queue_paused),
        current_job_id=str(current_job_id or ""),
        render_worker_active=bool(render_worker_active),
        scan_worker_active=bool(scan_worker_active),
        render_worker_stderr=str(render_worker_stderr or ""),
        scan_worker_stderr=str(scan_worker_stderr or ""),
        status_text=str(status_text or ""),
        recovery_headline=str(recovery_headline or ""),
    )
