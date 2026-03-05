from __future__ import annotations

import unittest

from queue_undo_redo import can_pop_history_for_shortcut, pop_history_for_shortcut


class QueueUndoRedoTests(unittest.TestCase):
    def test_can_pop_history_for_shortcut(self) -> None:
        stack = [{"kind": "update_jobs"}]
        self.assertTrue(
            can_pop_history_for_shortcut(
                scan_in_progress=False,
                stack=stack,
                command_targets_active=lambda _cmd: False,
            )
        )
        self.assertFalse(
            can_pop_history_for_shortcut(
                scan_in_progress=True,
                stack=stack,
                command_targets_active=lambda _cmd: False,
            )
        )
        self.assertFalse(
            can_pop_history_for_shortcut(
                scan_in_progress=False,
                stack=[],
                command_targets_active=lambda _cmd: False,
            )
        )
        self.assertFalse(
            can_pop_history_for_shortcut(
                scan_in_progress=False,
                stack=stack,
                command_targets_active=lambda _cmd: True,
            )
        )

    def test_pop_history_for_shortcut(self) -> None:
        stack = [{"id": 1}, {"id": 2}]
        popped = pop_history_for_shortcut(
            stack,
            scan_in_progress=False,
            command_targets_active=lambda _cmd: False,
        )
        self.assertEqual(popped, {"id": 2})
        self.assertEqual(stack, [{"id": 1}])

        blocked = pop_history_for_shortcut(
            stack,
            scan_in_progress=False,
            command_targets_active=lambda _cmd: True,
        )
        self.assertIsNone(blocked)
        self.assertEqual(stack, [{"id": 1}])


if __name__ == "__main__":
    unittest.main()
