from __future__ import annotations

import unittest

from ui_core.theme_support import DEFAULT_THEME, build_app_stylesheet


class ThemeSupportTests(unittest.TestCase):
    def test_queue_tableview_styles_are_explicit(self) -> None:
        stylesheet = build_app_stylesheet(DEFAULT_THEME, {})
        self.assertIn("QTableView {", stylesheet)
        self.assertIn("selection-background-color: transparent;", stylesheet)
        self.assertIn("QTableView::item:selected", stylesheet)


if __name__ == "__main__":
    unittest.main()
