from __future__ import annotations

import unittest

from notification_coordinator import (
    appendable_notifications,
    appendable_notifications_for_log,
)


class NotificationCoordinatorTests(unittest.TestCase):
    def test_appendable_notifications_dedupes_consecutive(self) -> None:
        entries, next_signature = appendable_notifications(
            candidates=[
                ("Queue started.", "info"),
                ("Queue started.", "info"),
                ("Queue complete.", "info"),
            ],
            last_signature=None,
            dedupe_consecutive=True,
        )
        self.assertEqual(entries, [("Queue started.", "info"), ("Queue complete.", "info")])
        self.assertEqual(next_signature, ("Queue complete.", "info"))

    def test_appendable_notifications_respects_previous_signature(self) -> None:
        entries, next_signature = appendable_notifications(
            candidates=[("Queue complete.", "info"), ("Queue stopped.", "warning")],
            last_signature=("Queue complete.", "info"),
            dedupe_consecutive=True,
        )
        self.assertEqual(entries, [("Queue stopped.", "warning")])
        self.assertEqual(next_signature, ("Queue stopped.", "warning"))

    def test_appendable_notifications_for_log_uses_parser(self) -> None:
        entries, next_signature = appendable_notifications_for_log(
            source="Info",
            text="=== Queue Started ===\n=== Queue Started ===\n=== Queue Complete ===\n",
            last_signature=None,
            dedupe_consecutive=True,
        )
        self.assertEqual(entries, [("Queue started.", "info"), ("Queue complete.", "info")])
        self.assertEqual(next_signature, ("Queue complete.", "info"))


if __name__ == "__main__":
    unittest.main()
