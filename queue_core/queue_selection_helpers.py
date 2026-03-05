from __future__ import annotations

from typing import Any, Callable


def source_rows_from_view_rows(
    view_rows: list[int],
    *,
    source_row_for_view_row: Callable[[int], int],
    job_count: int,
) -> list[int]:
    source_rows = [source_row_for_view_row(row) for row in view_rows]
    return sorted({row for row in source_rows if 0 <= row < job_count})


def selected_row_from_view_rows(
    view_rows: list[int],
    *,
    source_row_for_view_row: Callable[[int], int],
) -> int:
    if not view_rows:
        return -1
    return int(source_row_for_view_row(int(view_rows[0])))


def mixed_value(values: list[Any]) -> tuple[bool, Any]:
    if not values:
        return False, None
    first = values[0]
    return any(value != first for value in values[1:]), first
