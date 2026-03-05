from __future__ import annotations

from typing import Any, Callable


def can_pop_history_for_shortcut(
    *,
    scan_in_progress: bool,
    stack: list[dict[str, Any]],
    command_targets_active: Callable[[dict[str, Any]], bool],
) -> bool:
    if scan_in_progress:
        return False
    if not stack:
        return False
    return not command_targets_active(stack[-1])


def pop_history_for_shortcut(
    stack: list[dict[str, Any]],
    *,
    scan_in_progress: bool,
    command_targets_active: Callable[[dict[str, Any]], bool],
) -> dict[str, Any] | None:
    if not can_pop_history_for_shortcut(
        scan_in_progress=scan_in_progress,
        stack=stack,
        command_targets_active=command_targets_active,
    ):
        return None
    return stack.pop()
