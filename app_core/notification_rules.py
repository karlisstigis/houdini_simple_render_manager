from __future__ import annotations


def classified_render_error_notification(low: str) -> tuple[str, str] | None:
    if not low:
        return None
    gpu_oom_tokens = (
        "cuda error: out of memory",
        "cuda error out of memory",
        "hiperroroutofmemory",
        "vk_error_out_of_device_memory",
        "out of device memory",
        "optix_error_memory_allocation_failure",
        "gpu out of memory",
    )
    if any(token in low for token in gpu_oom_tokens) or (
        "out of memory" in low and any(tag in low for tag in ("gpu", "cuda", "optix", "xpu", "vram"))
    ):
        return ("Render failed: GPU out of memory.", "error")

    gpu_lost_tokens = (
        "vk_error_device_lost",
        "device lost",
        "gpu crash",
        "driver reset",
        "cuda_error_unknown",
        "xid ",
        "dxgi_error_device_removed",
        "tdr",
    )
    if any(token in low for token in gpu_lost_tokens):
        return ("Render failed: GPU device lost or driver reset.", "error")

    gpu_missing_tokens = (
        "no cuda-capable device",
        "no compatible gpu",
        "no gpu devices found",
        "no devices found for xpu",
        "failed to initialize cuda",
        "failed to initialize optix",
    )
    if any(token in low for token in gpu_missing_tokens):
        return ("Render failed: no compatible GPU device available.", "error")

    ram_oom_tokens = (
        "std::bad_alloc",
        "cannot allocate memory",
        "memory allocation failed",
        "fatal error: out of memory",
    )
    if any(token in low for token in ram_oom_tokens):
        return ("Render failed: system memory exhausted.", "error")
    return None


def notification_summary_for_line(source: str, line: str) -> tuple[str, str] | None:
    low = str(line or "").lower()
    classified_error = classified_render_error_notification(low)
    if classified_error is not None:
        return classified_error
    if line.startswith("===") and line.endswith("==="):
        inner = line.strip("=").strip()
        inner_low = inner.lower()
        if inner_low == "queue started":
            return ("Queue started.", "info")
        if inner_low == "queue complete":
            return ("Queue complete.", "info")
        if inner_low == "queue stopped":
            return ("Queue stopped.", "warning")
        if inner_low == "queue aborted":
            return ("Queue aborted.", "error")
        if inner_low.startswith("render start:"):
            job_name = inner.split(":", 1)[1].strip() if ":" in inner else inner
            return (f"Started render: {job_name}", "info")
        return None
    if line.startswith("[Queue] Stop requested"):
        return ("Stopping queue after the current step.", "warning")
    if line.startswith("[Queue] Terminating current render process"):
        return ("Stopping the active render.", "warning")
    if line.startswith("[Queue] Force killing current render process"):
        return ("Force-stopped the active render.", "error")
    if line.startswith("[Queue] Resumed"):
        return ("Queue resumed.", "info")
    if line.startswith("[Queue] Pause requested"):
        return ("Queue will pause after the current job.", "warning")
    if line.startswith("[Scan] No likely render/output nodes matched"):
        return ("No likely render nodes were found. Showing all scanned nodes.", "warning")
    if line.startswith("[Retry] "):
        return ("Retrying the current render chunk after a worker failure.", "warning")
    if line.startswith("[Preflight] Failed"):
        return ("Render preflight failed.", "error")
    if line.startswith("[Queue] Failed to save queue"):
        return ("Failed to save the queue file.", "error")
    if line.startswith("[Queue] Failed to load queue"):
        return ("Failed to load the queue file.", "error")
    if line.startswith("[Queue] Failed to start render worker"):
        return ("Failed to start the render worker.", "error")
    if line.startswith("[Log] Failed to open log file"):
        return ("Failed to open the job log file.", "error")
    if "unresponsive" in low and "worker" in low:
        if "render worker" in low:
            return ("The render worker stopped responding.", "error")
        if "scan worker" in low:
            return ("The scan worker stopped responding.", "error")
        return ("A worker process stopped responding.", "error")
    if "worker exited unexpectedly" in low:
        if "render worker" in low:
            return ("The render worker exited unexpectedly.", "error")
        if "scan worker" in low:
            return ("The scan worker exited unexpectedly.", "error")
        return ("A worker process exited unexpectedly.", "error")
    if source.lower() == "stderr" and any(token in low for token in ("traceback", "error", "failed", "interrupted")):
        if "interrupted" in low:
            return ("A render was interrupted.", "warning")
        if "traceback" in low:
            return ("A technical error was reported. See Logs for details.", "error")
        return ("An error was reported. See Logs for details.", "error")
    return None


def notification_messages_for_log(source: str, text: str) -> list[tuple[str, str]]:
    messages: list[tuple[str, str]] = []
    source_label = str(source or "").strip() or "Info"
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[Recovery] "):
            continue
        summary = notification_summary_for_line(source_label, line)
        if summary is not None:
            messages.append(summary)
    return messages

