from __future__ import annotations

import unittest

from app_core.notification_list_state import (
    normalized_notification,
    notification_color_hex,
    notification_signature,
    should_add_notification,
    trim_notification_count,
)


class NotificationListStateTests(unittest.TestCase):
    def test_normalized_notification(self) -> None:
        self.assertIsNone(normalized_notification("", "info"))
        self.assertEqual(normalized_notification("  Hello ", "WARNING"), ("Hello", "warning"))

    def test_notification_signature(self) -> None:
        self.assertEqual(notification_signature("A", "error"), ("A", "error"))
        self.assertIsNone(notification_signature(" ", "error"))

    def test_should_add_notification(self) -> None:
        signature = ("A", "info")
        self.assertTrue(should_add_notification(signature=signature, last_signature=None, dedupe_consecutive=True))
        self.assertFalse(should_add_notification(signature=signature, last_signature=signature, dedupe_consecutive=True))
        self.assertTrue(should_add_notification(signature=signature, last_signature=signature, dedupe_consecutive=False))
        self.assertFalse(should_add_notification(signature=None, last_signature=None, dedupe_consecutive=False))

    def test_trim_notification_count(self) -> None:
        self.assertEqual(trim_notification_count(count=5, max_items=3), 2)
        self.assertEqual(trim_notification_count(count=2, max_items=3), 0)
        self.assertEqual(trim_notification_count(count=2, max_items=-1), 2)

    def test_notification_color_hex(self) -> None:
        self.assertEqual(notification_color_hex("error"), "#d96b6b")
        self.assertEqual(notification_color_hex("warning"), "#d4ad4a")
        self.assertEqual(notification_color_hex("info"), "#d8d8d8")


if __name__ == "__main__":
    unittest.main()
