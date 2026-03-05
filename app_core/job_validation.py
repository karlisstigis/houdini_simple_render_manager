from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationDecision:
    valid: bool
    title: str = ""
    message: str = ""


def _validate_resolved_frame_range(
    resolved: tuple[int, int, int] | None,
    *,
    offline: bool,
    title: str,
    missing_message: str,
    invalid_message: str,
) -> ValidationDecision:
    if resolved is None:
        if offline:
            return ValidationDecision(False, "", "")
        return ValidationDecision(False, title, missing_message)
    start_frame, end_frame, step = resolved
    if step <= 0 or end_frame < start_frame:
        return ValidationDecision(False, title, invalid_message)
    return ValidationDecision(True)


def validate_resume_from_output_inputs(*, strict_frame_range: bool) -> ValidationDecision:
    if strict_frame_range:
        return ValidationDecision(False, "Resume From Output", "Cannot resume from output on a Strict frame range ROP.")
    return ValidationDecision(True)


def validate_resolved_frame_range_for_resume(resolved: tuple[int, int, int] | None, *, offline: bool) -> ValidationDecision:
    return _validate_resolved_frame_range(
        resolved,
        offline=offline,
        title="Resume From Output",
        missing_message="Cannot resolve frame range for this job.",
        invalid_message="Invalid job frame range for resume.",
    )


def validate_resume_probe_path(*, probe_path: str, pattern_resolved: bool) -> ValidationDecision:
    if not probe_path or probe_path.lower() == "ip":
        return ValidationDecision(False, "Resume From Output", "Cannot resume: output path is unavailable.")
    if not pattern_resolved:
        return ValidationDecision(
            False,
            "Resume From Output",
            "Could not resolve a reliable output filename pattern from the ROP. Use Reload from ROP (or start a render once) so the app can capture the exact output pattern.",
        )
    return ValidationDecision(True)


def validate_render_missing_inputs(resolved: tuple[int, int, int] | None, *, offline: bool) -> ValidationDecision:
    return _validate_resolved_frame_range(
        resolved,
        offline=offline,
        title="Render Missing",
        missing_message="Cannot resolve frame range for this job.",
        invalid_message="Invalid job frame range.",
    )


def validate_render_missing_probe_path(*, probe_path: str, pattern_resolved: bool) -> ValidationDecision:
    if not probe_path or probe_path.lower() == "ip":
        return ValidationDecision(False, "Render Missing", "Cannot evaluate outputs: output path is unavailable.")
    if not pattern_resolved:
        return ValidationDecision(False, "Render Missing", "Could not resolve a reliable output filename pattern from the ROP.")
    return ValidationDecision(True)


def validate_preview_launch(*, preview_path_exists: bool, player_path_set: bool, player_exists: bool) -> ValidationDecision:
    if not preview_path_exists:
        return ValidationDecision(False, "Preview", "No previewable output path is available for this job.")
    if not player_path_set:
        return ValidationDecision(False, "Preview", "Configure a preview player path in Preferences first.")
    if not player_exists:
        return ValidationDecision(False, "Preview", "Preview player does not exist.")
    return ValidationDecision(True)


def validate_logs_folder_access(*, folder_ready: bool, create_failed: bool = False) -> ValidationDecision:
    if not folder_ready:
        title = "Logs Folder"
        message = "Failed to create logs folder." if create_failed else "Failed to access logs folder."
        return ValidationDecision(False, title, message)
    return ValidationDecision(True)


def validate_log_file_deletion(*, logs_busy: bool, has_logs: bool) -> ValidationDecision:
    if logs_busy:
        return ValidationDecision(False, "Logs Busy", "Cannot delete log files while a render is active.")
    if not has_logs:
        return ValidationDecision(False, "Logs", "No log files found.")
    return ValidationDecision(True)


def validate_output_folder_open(*, folder_exists: bool) -> ValidationDecision:
    if not folder_exists:
        return ValidationDecision(False, "Folder Missing", "Folder does not exist.")
    return ValidationDecision(True)
