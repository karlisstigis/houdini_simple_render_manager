from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

from queue_models import RenderJob


def selected_job_log_path(job: RenderJob | None) -> Path | None:
    if job is None:
        return None
    raw = str(job.runtime.log_file_path or "").strip()
    if not raw:
        return None
    return Path(raw)


def discover_log_files(logs_dir: Path) -> list[Path]:
    return sorted(path for path in logs_dir.glob("*.log") if path.is_file())


def delete_log_files(
    log_paths: Iterable[Path],
    *,
    unlink_path: Callable[[Path], None] | None = None,
) -> tuple[int, list[str]]:
    deleted = 0
    failed: list[str] = []
    unlink = unlink_path or (lambda path: path.unlink())
    for path in log_paths:
        try:
            unlink(path)
            deleted += 1
        except OSError as exc:
            failed.append(f"{path.name}: {exc}")
    return deleted, failed


def log_deletion_feedback(
    *,
    deleted: int,
    failed: list[str],
    max_failed_items: int = 20,
) -> tuple[str, str, str | None]:
    if failed:
        return (
            "Logs",
            f"Deleted {deleted} log file(s), but {len(failed)} failed.",
            "\n".join(failed[: max(1, int(max_failed_items))]),
        )
    return "Logs", f"Deleted {deleted} log file(s).", None
