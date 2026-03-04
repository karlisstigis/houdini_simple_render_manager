from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from queue_models import RenderJob


@dataclass
class RetainedUsdDeleteDirsResult:
    deleted_any: bool
    target_dirs: set[Path]
    error_dir: Path | None = None
    error: OSError | None = None


def first_retained_usd_folder(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return paths[0].parent


def delete_retained_usd_directories(paths: list[Path]) -> RetainedUsdDeleteDirsResult:
    target_dirs = {path.resolve().parent for path in paths}
    deleted_any = False
    for folder in target_dirs:
        try:
            import shutil

            shutil.rmtree(folder, ignore_errors=False)
            deleted_any = True
        except OSError as exc:
            return RetainedUsdDeleteDirsResult(
                deleted_any=deleted_any,
                target_dirs=target_dirs,
                error_dir=folder,
                error=exc,
            )
    return RetainedUsdDeleteDirsResult(deleted_any=deleted_any, target_dirs=target_dirs)


def clear_deleted_retained_usd_runtime(
    jobs: list[RenderJob],
    target_dirs: set[Path],
    *,
    clear_runtime: Callable[[RenderJob], None],
) -> list[str]:
    cleared_job_ids: list[str] = []
    for job in jobs:
        retained_path = str(job.runtime.retained_usd_path or "").strip()
        if not retained_path:
            continue
        try:
            resolved_dir = Path(retained_path).resolve().parent
        except (OSError, RuntimeError):
            resolved_dir = Path(retained_path).parent
        if resolved_dir in target_dirs:
            clear_runtime(job)
            cleared_job_ids.append(job.id)
    return cleared_job_ids
