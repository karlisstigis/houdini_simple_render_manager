from __future__ import annotations

import unittest
from pathlib import Path

from retained_usd_panel_state import (
    can_delete_retained_usd,
    multi_job_retained_usd_panel_state,
    retained_usd_panel_default_fields,
)


class RetainedUsdPanelStateTests(unittest.TestCase):
    def test_default_fields(self) -> None:
        state = retained_usd_panel_default_fields()
        self.assertEqual(state["retained_usd_path"], "")
        self.assertEqual(state["retained_usd_built_range"], "-")
        self.assertEqual(state["retained_usd_built_step"], "-")
        self.assertEqual(state["retained_usd_built_at"], "-")
        self.assertEqual(state["retained_usd_status"], "-")
        self.assertEqual(state["retained_usd_warning"], "")
        self.assertFalse(state["can_open"])
        self.assertFalse(state["can_delete"])

    def test_multi_state_with_paths(self) -> None:
        paths = [
            Path("E:/cache/a/job1/__render__.usd"),
            Path("E:/cache/a/job1/other.usd"),
            Path("E:/cache/b/job2/__render__.usd"),
        ]
        state = multi_job_retained_usd_panel_state(paths)
        self.assertEqual(state["retained_usd_path"], "2 USD folder(s)")
        self.assertEqual(state["retained_usd_status"], "3 file(s) available")
        self.assertFalse(state["can_open"])

    def test_can_delete_rules(self) -> None:
        self.assertTrue(
            can_delete_retained_usd(
                selected_count=1,
                retained_state_can_open=True,
                retained_paths_present=False,
                has_active_or_locked_job=False,
            )
        )
        self.assertFalse(
            can_delete_retained_usd(
                selected_count=1,
                retained_state_can_open=True,
                retained_paths_present=False,
                has_active_or_locked_job=True,
            )
        )
        self.assertTrue(
            can_delete_retained_usd(
                selected_count=3,
                retained_state_can_open=False,
                retained_paths_present=True,
                has_active_or_locked_job=False,
            )
        )


if __name__ == "__main__":
    unittest.main()
