from __future__ import annotations

import unittest

from queue_core.queue_header_grouping import (
    is_valid_queue_header_grouping,
    queue_column_widths_from_data,
    queue_header_visual_order,
    queue_hidden_columns_from_data,
)


class QueueHeaderGroupingTests(unittest.TestCase):
    def test_queue_header_visual_order(self) -> None:
        visual_to_logical = [2, 0, 1, 3]
        order = queue_header_visual_order(
            column_count=4,
            logical_index_for_visual=lambda visual: visual_to_logical[visual],
        )
        self.assertEqual(order, [2, 0, 1, 3])

    def test_queue_hidden_columns_from_data_filters_invalid(self) -> None:
        hidden = queue_hidden_columns_from_data([0, "2", "x", -1, 99], column_count=4)
        self.assertEqual(hidden, {0, 2})

    def test_queue_column_widths_from_data_filters_invalid_and_small(self) -> None:
        widths = queue_column_widths_from_data(
            {"0": 120, "1": "8", "2": "bad", "9": 30},
            column_count=4,
        )
        self.assertEqual(widths, {0: 120})

    def test_is_valid_queue_header_grouping_accepts_expected_layout(self) -> None:
        visual_map = {0: 0, 1: 1, 2: 2, 7: 6, 8: 7}
        hidden = {3, 4, 5, 6}
        is_valid = is_valid_queue_header_grouping(
            column_count=9,
            left_group={0, 1, 2, 3, 4, 5, 6},
            boundary_visual_index=6,
            is_hidden=lambda logical: logical in hidden,
            visual_index_for_logical=lambda logical: visual_map.get(logical, -1),
        )
        self.assertTrue(is_valid)

    def test_is_valid_queue_header_grouping_rejects_cross_group_move(self) -> None:
        visual_map = {0: 6, 1: 1, 2: 2, 7: 0, 8: 7}
        hidden = {3, 4, 5, 6}
        is_valid = is_valid_queue_header_grouping(
            column_count=9,
            left_group={0, 1, 2, 3, 4, 5, 6},
            boundary_visual_index=6,
            is_hidden=lambda logical: logical in hidden,
            visual_index_for_logical=lambda logical: visual_map.get(logical, -1),
        )
        self.assertFalse(is_valid)


if __name__ == "__main__":
    unittest.main()
