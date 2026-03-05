from __future__ import annotations

from typing import Any, Callable


def queue_header_visual_order(
    *,
    column_count: int,
    logical_index_for_visual: Callable[[int], int],
) -> list[int]:
    return [logical_index_for_visual(v) for v in range(column_count)]


def queue_hidden_columns_from_data(raw: Any, *, column_count: int) -> set[int]:
    result: set[int] = set()
    if not isinstance(raw, list):
        return result
    for value in raw:
        try:
            logical = int(value)
        except (TypeError, ValueError):
            continue
        if 0 <= logical < column_count:
            result.add(logical)
    return result


def queue_column_widths_from_data(raw: Any, *, column_count: int, min_width: int = 9) -> dict[int, int]:
    result: dict[int, int] = {}
    if not isinstance(raw, dict):
        return result
    for key, value in raw.items():
        try:
            logical = int(key)
            width = int(value)
        except (TypeError, ValueError):
            continue
        if 0 <= logical < column_count and width >= min_width:
            result[logical] = width
    return result


def is_valid_queue_header_grouping(
    *,
    column_count: int,
    left_group: set[int],
    boundary_visual_index: int,
    is_hidden: Callable[[int], bool],
    visual_index_for_logical: Callable[[int], int],
) -> bool:
    for logical in range(column_count):
        if is_hidden(logical):
            continue
        visual = visual_index_for_logical(logical)
        if visual < 0:
            continue
        if logical in left_group and visual >= boundary_visual_index:
            return False
        if logical not in left_group and visual < boundary_visual_index:
            return False
    return True
