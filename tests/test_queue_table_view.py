from __future__ import annotations

import unittest

from ui_core.widgets import QueueTableView


class QueueTableViewTests(unittest.TestCase):
    def test_autoscroll_step_uses_deadzone(self) -> None:
        self.assertEqual(QueueTableView._autoscroll_step_for_offset(0), 0)
        self.assertEqual(QueueTableView._autoscroll_step_for_offset(QueueTableView.AUTOSCROLL_DEADZONE_PX), 0)

    def test_autoscroll_step_preserves_direction(self) -> None:
        self.assertGreater(QueueTableView._autoscroll_step_for_offset(40), 0)
        self.assertLess(QueueTableView._autoscroll_step_for_offset(-40), 0)


if __name__ == "__main__":
    unittest.main()
