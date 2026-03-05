from __future__ import annotations


def usd_status_display(*, retained_path: str, retained_exists: bool, stale_reason: str) -> str:
    if not retained_path or not retained_exists:
        return "Build"
    if stale_reason:
        return "Rebuild"
    return "Reusable"


def usd_status_tooltip(
    *,
    retained_path: str,
    retained_exists: bool,
    stale_reason: str,
    reuse_retained_usd: bool,
) -> str:
    if not retained_path or not retained_exists:
        return "No retained USD is available for this job. USD will be built during render."
    if stale_reason:
        return stale_reason
    if not reuse_retained_usd:
        return "Retained USD is reusable, but 'Use existing USD files' is disabled."
    return "Retained USD can be reused on next render."
