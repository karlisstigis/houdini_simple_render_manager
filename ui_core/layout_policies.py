from __future__ import annotations

from typing import Iterable

from PySide6 import QtWidgets


LEFT_STACK_CONTENT_MARGIN = 0
LEFT_STACK_SCROLLBAR_GAP = 6
JOB_PROPERTIES_FALLBACK_MIN_WIDTH = 300


def splitter_total_size(sizes: Iterable[int]) -> int:
    return int(sum(max(0, int(size)) for size in sizes))


def widget_width_candidates(widget: QtWidgets.QWidget | None) -> list[int]:
    if not isinstance(widget, QtWidgets.QWidget):
        return []
    candidates: list[int] = []
    try:
        candidates.append(int(widget.minimumSizeHint().width()))
    except Exception:
        pass
    try:
        hint_width = int(widget.sizeHint().width())
        if hint_width > 0:
            candidates.append(hint_width)
    except Exception:
        pass
    try:
        minimum_width = int(widget.minimumWidth())
        if minimum_width > 0:
            candidates.append(minimum_width)
    except Exception:
        pass
    return [width for width in candidates if width > 0]


def stable_widget_width_candidates(widget: QtWidgets.QWidget | None) -> list[int]:
    if not isinstance(widget, QtWidgets.QWidget):
        return []
    candidates: list[int] = []
    try:
        candidates.append(int(widget.minimumSizeHint().width()))
    except Exception:
        pass
    try:
        minimum_width = int(widget.minimumWidth())
        if minimum_width > 0:
            candidates.append(minimum_width)
    except Exception:
        pass
    return [width for width in candidates if width > 0]


def left_pane_content_required_width(
    stack_host: QtWidgets.QWidget | None,
    panels: Iterable[QtWidgets.QWidget | None],
) -> int | None:
    width_candidates = stable_widget_width_candidates(stack_host)
    for panel in panels:
        width_candidates.extend(stable_widget_width_candidates(panel))
    if not width_candidates:
        return None
    return int(max(width_candidates))


def left_pane_required_width(
    *,
    content_required_width: int | None,
    scrollbar_visible: bool,
    scrollbar_extent: int,
    content_margin: int = LEFT_STACK_CONTENT_MARGIN,
    scrollbar_gap: int = LEFT_STACK_SCROLLBAR_GAP,
    reserve_scrollbar_space: bool = True,
) -> int | None:
    if content_required_width is None:
        return None
    reserve_scrollbar_footprint = bool(reserve_scrollbar_space or scrollbar_visible)
    return int(
        content_required_width
        + (int(content_margin) * 2)
        + (int(scrollbar_gap) if reserve_scrollbar_footprint else 0)
        + (int(scrollbar_extent) if reserve_scrollbar_footprint else 0)
    )


def left_pane_content_floor(
    *,
    pane_floor: int,
    content_floor: int | None,
    scrollbar_visible: bool,
    scrollbar_extent: int,
    content_margin: int = LEFT_STACK_CONTENT_MARGIN,
    scrollbar_gap: int = LEFT_STACK_SCROLLBAR_GAP,
) -> int:
    if content_floor is not None and content_floor > 0:
        return int(content_floor)
    computed = max(1, int(pane_floor) - (int(content_margin) * 2))
    if scrollbar_visible:
        computed = max(1, computed - int(scrollbar_gap) - int(scrollbar_extent))
    return int(computed)


def job_properties_min_width(
    widgets: Iterable[QtWidgets.QWidget | None],
    *,
    fallback: int = JOB_PROPERTIES_FALLBACK_MIN_WIDTH,
) -> int:
    width_candidates: list[int] = []
    for widget in widgets:
        width_candidates.extend(widget_width_candidates(widget))
    if not width_candidates:
        return int(fallback)
    return int(max(width_candidates))


def job_properties_target_width(
    *,
    current_width: int,
    total_width: int,
    remembered_width: int | None,
    min_width: int,
) -> int:
    desired_width = int(remembered_width or current_width)
    return max(0, min(max(int(min_width), desired_width), max(0, int(total_width) - 1)))
