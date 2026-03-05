from __future__ import annotations

import unittest

from queue_selection_helpers import mixed_value, selected_row_from_view_rows, source_rows_from_view_rows


class QueueSelectionHelpersTests(unittest.TestCase):
    def test_source_rows_from_view_rows_filters_invalid_and_dedupes(self) -> None:
        mapped = {0: 3, 1: 2, 2: -1, 3: 2}
        rows = source_rows_from_view_rows(
            [0, 1, 2, 3],
            source_row_for_view_row=lambda view_row: mapped.get(view_row, -1),
            job_count=4,
        )
        self.assertEqual(rows, [2, 3])

    def test_selected_row_from_view_rows(self) -> None:
        self.assertEqual(
            selected_row_from_view_rows([5, 6], source_row_for_view_row=lambda view_row: view_row - 2),
            3,
        )
        self.assertEqual(selected_row_from_view_rows([], source_row_for_view_row=lambda view_row: view_row), -1)

    def test_mixed_value(self) -> None:
        self.assertEqual(mixed_value([]), (False, None))
        self.assertEqual(mixed_value([1, 1, 1]), (False, 1))
        self.assertEqual(mixed_value([1, 2, 1]), (True, 1))


if __name__ == "__main__":
    unittest.main()
