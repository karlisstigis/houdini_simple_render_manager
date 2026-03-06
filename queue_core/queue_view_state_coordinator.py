from __future__ import annotations

from typing import Any

from queue_core.queue_header_grouping import (
    is_valid_queue_header_grouping as is_valid_queue_header_grouping_model,
    queue_column_widths_from_data as queue_column_widths_from_data_model,
    queue_header_visual_order as queue_header_visual_order_model,
    queue_hidden_columns_from_data as queue_hidden_columns_from_data_model,
    sanitized_queue_column_width as sanitized_queue_column_width_model,
)


class QueueViewStateCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def header_visual_order(self) -> list[int]:
        header = self._w.queue_table.horizontalHeader()
        return queue_header_visual_order_model(
            column_count=self._w.queue_table.columnCount(),
            logical_index_for_visual=header.logicalIndex,
        )

    def hidden_columns_from_data(self, raw: Any) -> set[int]:
        return queue_hidden_columns_from_data_model(
            raw,
            column_count=self._w.queue_table.columnCount(),
        )

    def default_column_width(self, logical: int, fallback: int) -> int:
        default_widths = getattr(self._w, "_queue_default_column_widths", {})
        return int(default_widths.get(logical, max(40, int(fallback))))

    def sanitized_column_width(self, logical: int, width: int) -> int:
        default_width = self.default_column_width(logical, width)
        viewport_width = 0
        try:
            viewport_width = int(self._w.queue_table.viewport().width())
        except Exception:
            viewport_width = 0
        return sanitized_queue_column_width_model(
            logical=logical,
            width=width,
            default_width=default_width,
            viewport_width=viewport_width,
        )

    def column_widths_from_data(self, raw: Any) -> dict[int, int]:
        parsed = queue_column_widths_from_data_model(
            raw,
            column_count=self._w.queue_table.columnCount(),
        )
        return {
            logical: self.sanitized_column_width(logical, width)
            for logical, width in parsed.items()
        }

    def reset_to_defaults(self) -> None:
        default_widths = getattr(self._w, "_queue_default_column_widths", {})
        for logical in range(self._w.queue_table.columnCount()):
            width = int(default_widths.get(logical, self._w.queue_table.columnWidth(logical)))
            self._w.queue_table.setColumnWidth(logical, width)
            self._w.queue_table.setColumnHidden(logical, False)

    def apply_persisted_data(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            return
        widths = self.column_widths_from_data(raw.get("column_widths", {}))
        for logical, width in widths.items():
            self._w.queue_table.setColumnWidth(logical, width)

        hidden = self.hidden_columns_from_data(raw.get("hidden_columns", []))
        visible_count = self._w.queue_table.columnCount() - len(hidden)
        if visible_count <= 0:
            hidden.clear()
        for logical in range(self._w.queue_table.columnCount()):
            self._w.queue_table.setColumnHidden(logical, logical in hidden)

    def is_valid_header_grouping(self) -> bool:
        header = self._w.queue_table.horizontalHeader()
        return is_valid_queue_header_grouping_model(
            column_count=self._w.queue_table.columnCount(),
            left_group={0, 1, 2, 3, 4, 5, 6},
            boundary_visual_index=6,
            is_hidden=self._w.queue_table.isColumnHidden,
            visual_index_for_logical=header.visualIndex,
        )

    def restore_header_order(self, visual_order: list[int]) -> None:
        header = self._w.queue_table.horizontalHeader()
        self._w._queue_header_group_restore_guard = True
        try:
            for target_visual, logical in enumerate(visual_order):
                current_visual = header.visualIndex(logical)
                if current_visual != target_visual:
                    header.moveSection(current_visual, target_visual)
        finally:
            self._w._queue_header_group_restore_guard = False
            self._w.queue_table.viewport().update()

    def on_header_section_moved(self) -> None:
        if self._w._queue_header_group_restore_guard:
            return
        if self.is_valid_header_grouping():
            self._w._queue_header_valid_order = self.header_visual_order()
            self._w.queue_table.viewport().update()
            return
        if self._w._queue_header_valid_order:
            self.restore_header_order(self._w._queue_header_valid_order)
