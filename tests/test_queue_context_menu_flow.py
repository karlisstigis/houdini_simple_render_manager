from __future__ import annotations

import unittest

from queue_context_menu_flow import (
    apply_job_mutation_with_history,
    build_queue_context_menu_availability,
    queue_context_action_key,
)
from queue_models import RenderJob


class QueueContextMenuFlowTests(unittest.TestCase):
    def test_build_queue_context_menu_availability(self) -> None:
        state = build_queue_context_menu_availability(
            job_enabled=True,
            any_active=False,
            any_locked=False,
            has_finished_jobs=True,
            reset_value_allowed=True,
            reload_allowed=True,
            duplicate_allowed=False,
            preview_allowed=True,
            open_folder_allowed=False,
        )
        self.assertEqual(state.toggle_text, "Disable")
        self.assertTrue(state.toggle_enabled)
        self.assertTrue(state.reset_enabled)
        self.assertTrue(state.reset_value_enabled)
        self.assertTrue(state.reload_enabled)
        self.assertFalse(state.duplicate_enabled)
        self.assertTrue(state.clear_finished_enabled)
        self.assertTrue(state.preview_enabled)
        self.assertFalse(state.open_folder_enabled)

    def test_queue_context_action_key(self) -> None:
        action_a = object()
        action_b = object()
        key = queue_context_action_key(action_b, {"a": action_a, "b": action_b})
        self.assertEqual(key, "b")
        self.assertIsNone(queue_context_action_key(object(), {"a": action_a}))

    def test_apply_job_mutation_with_history(self) -> None:
        first = RenderJob("E:/a.hip", "/stage/A", "use_rop")
        second = RenderJob("E:/b.hip", "/stage/B", "use_rop")
        first.enabled = True
        second.enabled = True
        history: list[dict] = []
        saves: list[list[str]] = []

        changed = apply_job_mutation_with_history(
            [first, second],
            is_active_job=lambda job: job is second,
            mutate_job=lambda job: setattr(job, "enabled", False),
            job_states_for_ids=lambda ids: [{"id": job_id} for job_id in ids],
            push_history_command=lambda payload: history.append(dict(payload)),
            save_and_refresh_queue=lambda ids: saves.append(list(ids)),
        )

        self.assertTrue(changed)
        self.assertFalse(first.enabled)
        self.assertTrue(second.enabled)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["kind"], "update_jobs")
        self.assertEqual(saves, [[first.id, second.id]])


if __name__ == "__main__":
    unittest.main()
