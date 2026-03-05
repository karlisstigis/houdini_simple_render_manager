from __future__ import annotations

import unittest

from usd_core.usd_queue_status import usd_status_display, usd_status_tooltip


class UsdQueueStatusTests(unittest.TestCase):
    def test_missing_retained_usd_status(self) -> None:
        self.assertEqual(
            usd_status_display(retained_path="", retained_exists=False, stale_reason=""),
            "Build",
        )
        self.assertIn(
            "No retained USD is available",
            usd_status_tooltip(
                retained_path="",
                retained_exists=False,
                stale_reason="",
                reuse_retained_usd=False,
            ),
        )

    def test_stale_retained_usd_status(self) -> None:
        reason = "Cannot reuse USD: current frame range exceeds the built USD range."
        self.assertEqual(
            usd_status_display(retained_path="D:/usd/render.usd", retained_exists=True, stale_reason=reason),
            "Rebuild",
        )
        self.assertEqual(
            usd_status_tooltip(
                retained_path="D:/usd/render.usd",
                retained_exists=True,
                stale_reason=reason,
                reuse_retained_usd=True,
            ),
            reason,
        )

    def test_reusable_retained_usd_status(self) -> None:
        self.assertEqual(
            usd_status_display(retained_path="D:/usd/render.usd", retained_exists=True, stale_reason=""),
            "Reusable",
        )
        self.assertIn(
            "disabled",
            usd_status_tooltip(
                retained_path="D:/usd/render.usd",
                retained_exists=True,
                stale_reason="",
                reuse_retained_usd=False,
            ),
        )
        self.assertIn(
            "can be reused",
            usd_status_tooltip(
                retained_path="D:/usd/render.usd",
                retained_exists=True,
                stale_reason="",
                reuse_retained_usd=True,
            ),
        )


if __name__ == "__main__":
    unittest.main()
