"""Shared queue/tree path propagation and ROP metadata refresh helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from queue_editing import apply_queue_path_text
from queue_models import RenderJob
from rop_metadata import RopInfo, apply_rop_info_to_job as apply_rop_info_to_job_model


def validate_queue_path_value(column: int, text: str) -> str:
    value = str(text or "").strip()
    if column == 1:
        if not value:
            raise ValueError("HIP path cannot be empty.")
        return value
    if column == 2:
        if not value:
            raise ValueError("ROP path cannot be empty.")
        if not value.startswith("/"):
            raise ValueError("ROP path should look like /out/my_rop.")
        return value
    return value


def sync_job_after_path_change(
    job: RenderJob,
    *,
    probe_cache: dict[tuple[str, str], RopInfo | None],
    probe_rop_info: Callable[[str, str], RopInfo | None],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    restore_job_online_status: Callable[[RenderJob], None],
    normalize_output_display_path: Callable[[str], str],
) -> None:
    hip_path = str(job.spec.hip_path or "").strip()
    rop_path = str(job.spec.rop_path or "").strip()
    if not hip_path or not Path(hip_path).exists():
        mark_job_offline(job, "HIP file not found.")
        return
    try:
        key = (hip_path, rop_path)
        if key not in probe_cache:
            probe_cache[key] = probe_rop_info(hip_path, rop_path)
        info = probe_cache.get(key)
        if info is None:
            return
        if info.error == "node_not_found":
            mark_job_offline(job, "ROP node not found in HIP file.")
            return
        apply_rop_info_to_job_model(
            job,
            info,
            normalize_output_display_path,
            apply_runtime_range=True,
        )
        restore_job_online_status(job)
    except Exception as exc:
        mark_job_offline(job, f"Failed to refresh ROP metadata: {exc}")


def apply_hip_path_change(
    jobs: list[RenderJob],
    *,
    old_hip: str,
    new_hip: str,
    running_status: Any,
) -> list[RenderJob]:
    old_key = str(old_hip or "").strip()
    new_key = str(new_hip or "").strip()
    if old_key == new_key:
        return []
    changed_jobs: list[RenderJob] = []
    for job in jobs:
        if job.runtime.status == running_status:
            continue
        if str(job.spec.hip_path or "").strip() != old_key:
            continue
        try:
            apply_queue_path_text(job, 1, new_key)
        except ValueError:
            continue
        changed_jobs.append(job)
    return changed_jobs


def apply_rop_path_change(
    jobs: list[RenderJob],
    *,
    hip_path: str,
    old_rop: str,
    new_rop: str,
    running_status: Any,
) -> list[RenderJob]:
    hip_key = str(hip_path or "").strip()
    old_key = str(old_rop or "").strip()
    new_key = str(new_rop or "").strip()
    if old_key == new_key:
        return []
    changed_jobs: list[RenderJob] = []
    for job in jobs:
        if job.runtime.status == running_status:
            continue
        if str(job.spec.hip_path or "").strip() != hip_key:
            continue
        if str(job.spec.rop_path or "").strip() != old_key:
            continue
        try:
            apply_queue_path_text(job, 2, new_key)
        except ValueError:
            continue
        changed_jobs.append(job)
    return changed_jobs


def sync_jobs_after_path_change(
    jobs: list[RenderJob],
    *,
    probe_rop_info: Callable[[str, str], RopInfo | None],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    restore_job_online_status: Callable[[RenderJob], None],
    normalize_output_display_path: Callable[[str], str],
) -> None:
    probe_cache: dict[tuple[str, str], RopInfo | None] = {}
    for job in jobs:
        sync_job_after_path_change(
            job,
            probe_cache=probe_cache,
            probe_rop_info=probe_rop_info,
            mark_job_offline=mark_job_offline,
            restore_job_online_status=restore_job_online_status,
            normalize_output_display_path=normalize_output_display_path,
        )


def propagate_hip_path_change(
    jobs: list[RenderJob],
    *,
    old_hip: str,
    new_hip: str,
    running_status: Any,
    probe_rop_info: Callable[[str, str], RopInfo | None],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    restore_job_online_status: Callable[[RenderJob], None],
    normalize_output_display_path: Callable[[str], str],
) -> list[str]:
    changed_jobs = apply_hip_path_change(
        jobs,
        old_hip=old_hip,
        new_hip=new_hip,
        running_status=running_status,
    )
    sync_jobs_after_path_change(
        changed_jobs,
        probe_rop_info=probe_rop_info,
        mark_job_offline=mark_job_offline,
        restore_job_online_status=restore_job_online_status,
        normalize_output_display_path=normalize_output_display_path,
    )
    return [job.id for job in changed_jobs]


def propagate_rop_path_change(
    jobs: list[RenderJob],
    *,
    hip_path: str,
    old_rop: str,
    new_rop: str,
    running_status: Any,
    probe_rop_info: Callable[[str, str], RopInfo | None],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    restore_job_online_status: Callable[[RenderJob], None],
    normalize_output_display_path: Callable[[str], str],
) -> list[str]:
    changed_jobs = apply_rop_path_change(
        jobs,
        hip_path=hip_path,
        old_rop=old_rop,
        new_rop=new_rop,
        running_status=running_status,
    )
    sync_jobs_after_path_change(
        changed_jobs,
        probe_rop_info=probe_rop_info,
        mark_job_offline=mark_job_offline,
        restore_job_online_status=restore_job_online_status,
        normalize_output_display_path=normalize_output_display_path,
    )
    return [job.id for job in changed_jobs]


def refresh_jobs_from_rop_metadata(
    target_jobs: list[RenderJob],
    *,
    running_status: Any,
    scan_rop_info_for_hip: Callable[[str], dict[str, RopInfo]],
    probe_rop_info: Callable[[str, str], RopInfo | None],
    mark_job_offline: Callable[[RenderJob, str | None], None],
    restore_job_online_status: Callable[[RenderJob], None],
    clear_job_resume_runtime_state: Callable[[RenderJob], None],
    normalize_output_display_path: Callable[[str], str],
    reset_override_to_rop: bool = False,
) -> list[str]:
    changed_ids: list[str] = []
    by_hip: dict[str, list[RenderJob]] = {}
    for target in target_jobs:
        if target.runtime.status == running_status:
            continue
        by_hip.setdefault(target.spec.hip_path, []).append(target)

    for hip_path, hip_jobs in by_hip.items():
        if not Path(hip_path).exists():
            for target in hip_jobs:
                mark_job_offline(target, "HIP file not found.")
                changed_ids.append(target.id)
            continue
        scan_info_map = scan_rop_info_for_hip(hip_path)
        single_probe_cache: dict[str, RopInfo | None] = {}
        for target in hip_jobs:
            try:
                info = scan_info_map.get(target.spec.rop_path)
                if info is None:
                    if target.spec.rop_path not in single_probe_cache:
                        single_probe_cache[target.spec.rop_path] = probe_rop_info(target.spec.hip_path, target.spec.rop_path)
                    info = single_probe_cache[target.spec.rop_path]
                if info is None:
                    continue
                if info.error == "node_not_found":
                    mark_job_offline(target, "ROP node not found in HIP file.")
                    changed_ids.append(target.id)
                    continue
                if reset_override_to_rop:
                    target.spec.frame_range_mode = "use_rop"
                    target.spec.start_frame = None
                    target.spec.end_frame = None
                    target.spec.step = None
                    clear_job_resume_runtime_state(target)
                apply_rop_info_to_job_model(
                    target,
                    info,
                    normalize_output_display_path,
                    apply_runtime_range=True,
                )
                restore_job_online_status(target)
                changed_ids.append(target.id)
            except Exception as exc:
                mark_job_offline(target, f"Failed to refresh ROP metadata: {exc}")
                changed_ids.append(target.id)
    return changed_ids
