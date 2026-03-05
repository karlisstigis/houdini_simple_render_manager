from __future__ import annotations


def build_ui_state(
    *,
    queue_active: bool,
    queue_paused: bool,
    render_job_active: bool,
    scan_in_progress: bool,
    create_job_scan_in_progress: bool,
    hbatch_ok: bool,
    path_sync_in_progress: bool,
    experimental_chunking_enabled: bool,
    chunking_checked: bool,
    has_queued: bool,
    can_start_selected: bool,
    selected_has_log: bool,
) -> dict[str, object]:
    running = bool(queue_active and render_job_active)
    start_enabled = bool(hbatch_ok and (has_queued or can_start_selected or (queue_active and queue_paused)))
    pause_text = "Resume" if queue_paused else "Pause"
    reload_all_enabled = bool(hbatch_ok and not scan_in_progress and not render_job_active and not path_sync_in_progress)
    queue_file_menu_enabled = bool(not scan_in_progress and not render_job_active and not path_sync_in_progress)
    force_disable_chunking = bool(not experimental_chunking_enabled and chunking_checked)
    chunk_checkbox_enabled = bool(experimental_chunking_enabled and not queue_active and not render_job_active)
    chunk_size_enabled = bool(chunk_checkbox_enabled and chunking_checked)
    idle_render_controls = bool(not queue_active and not render_job_active)
    status_message = None
    if running:
        status_message = "Rendering..."
    elif queue_active and queue_paused:
        status_message = "Queue paused"
    elif create_job_scan_in_progress:
        status_message = "Scanning /out ..."
    elif path_sync_in_progress:
        status_message = "Updating path..."
    return {
        "start_enabled": start_enabled,
        "pause_enabled": bool(queue_active),
        "pause_text": pause_text,
        "stop_enabled": bool(queue_active or render_job_active),
        "reload_all_enabled": reload_all_enabled,
        "queue_file_menu_enabled": queue_file_menu_enabled,
        "disable_husk_mplay_enabled": idle_render_controls,
        "chunking_visible": bool(experimental_chunking_enabled),
        "force_disable_chunking": force_disable_chunking,
        "chunk_checkbox_enabled": chunk_checkbox_enabled,
        "chunk_size_enabled": chunk_size_enabled,
        "auto_retry_enabled": idle_render_controls,
        "retry_delay_enabled": idle_render_controls,
        "selected_has_log": bool(selected_has_log),
        "status_message": status_message,
    }
