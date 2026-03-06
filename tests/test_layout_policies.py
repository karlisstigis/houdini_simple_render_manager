from __future__ import annotations

import unittest

from ui_core.layout_policies import left_pane_required_width


class LayoutPoliciesTests(unittest.TestCase):
    def test_left_pane_required_width_reserves_scrollbar_space_even_when_hidden(self) -> None:
        self.assertEqual(
            left_pane_required_width(
                content_required_width=240,
                scrollbar_visible=False,
                scrollbar_extent=14,
                content_margin=0,
                scrollbar_gap=6,
            ),
            260,
        )

    def test_left_pane_required_width_can_disable_reserved_scrollbar_space(self) -> None:
        self.assertEqual(
            left_pane_required_width(
                content_required_width=240,
                scrollbar_visible=False,
                scrollbar_extent=14,
                content_margin=0,
                scrollbar_gap=6,
                reserve_scrollbar_space=False,
            ),
            240,
        )


if __name__ == "__main__":
    unittest.main()
