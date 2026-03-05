from __future__ import annotations

import unittest

from queue_history import (
    apply_history_command,
    bounded_undo_stack,
    history_command_candidate_ids,
    history_command_targets_job,
    should_push_history_command,
)


class QueueHistoryTests(unittest.TestCase):
    def test_should_push_history_command(self) -> None:
        self.assertFalse(should_push_history_command(history_applying=True, command={"kind": "update_jobs"}))
        self.assertFalse(should_push_history_command(history_applying=False, command={"kind": "insert_jobs", "entries": []}))
        self.assertFalse(
            should_push_history_command(
                history_applying=False,
                command={"kind": "update_jobs", "before": [{"id": "a"}], "after": [{"id": "a"}]},
            )
        )
        self.assertTrue(
            should_push_history_command(
                history_applying=False,
                command={"kind": "update_jobs", "before": [{"id": "a"}], "after": [{"id": "b"}]},
            )
        )

    def test_bounded_undo_stack(self) -> None:
        stack = [{"i": i} for i in range(5)]
        self.assertEqual(bounded_undo_stack(stack, max_size=10), stack)
        self.assertEqual(bounded_undo_stack(stack, max_size=3), [{"i": 2}, {"i": 3}, {"i": 4}])

    def test_history_command_candidate_ids(self) -> None:
        command = {
            "before": [{"id": "a"}],
            "after": [{"id": "b"}],
            "entries": [{"job": {"id": "c"}}],
            "before_order": ["d"],
            "after_order": ["e"],
            "undo_select_job_ids": ["f"],
            "redo_select_job_ids": ["g"],
        }
        self.assertEqual(history_command_candidate_ids(command), {"a", "b", "c", "d", "e", "f", "g"})
        self.assertTrue(history_command_targets_job(command, active_job_id="c"))
        self.assertFalse(history_command_targets_job(command, active_job_id=""))
        self.assertFalse(history_command_targets_job(command, active_job_id="z"))

    def test_apply_history_command_insert_remove_and_select_ids(self) -> None:
        calls: list[tuple[str, object]] = []
        command = {
            "kind": "insert_jobs",
            "entries": [{"job": {"id": "x"}}],
            "undo_select_job_ids": ["undo"],
            "redo_select_job_ids": ["redo"],
        }
        select_ids = apply_history_command(
            command,
            undo=True,
            remove_jobs_by_ids=lambda ids: calls.append(("remove", list(ids))),
            insert_jobs_from_entries=lambda entries: calls.append(("insert", list(entries))),
            apply_job_states=lambda states: calls.append(("states", list(states))),
            apply_job_order=lambda order: calls.append(("order", list(order))),
        )
        self.assertEqual(calls, [("remove", ["x"])])
        self.assertEqual(select_ids, ["undo"])

    def test_apply_history_command_update_and_reorder(self) -> None:
        calls: list[tuple[str, object]] = []
        update = {
            "kind": "update_jobs",
            "before": [{"id": "a", "v": 1}],
            "after": [{"id": "a", "v": 2}],
            "undo_select_job_ids": ["a"],
            "redo_select_job_ids": ["a"],
        }
        apply_history_command(
            update,
            undo=False,
            remove_jobs_by_ids=lambda ids: calls.append(("remove", list(ids))),
            insert_jobs_from_entries=lambda entries: calls.append(("insert", list(entries))),
            apply_job_states=lambda states: calls.append(("states", list(states))),
            apply_job_order=lambda order: calls.append(("order", list(order))),
        )
        reorder = {
            "kind": "reorder_jobs",
            "before_order": ["a", "b"],
            "after_order": ["b", "a"],
        }
        apply_history_command(
            reorder,
            undo=True,
            remove_jobs_by_ids=lambda ids: calls.append(("remove", list(ids))),
            insert_jobs_from_entries=lambda entries: calls.append(("insert", list(entries))),
            apply_job_states=lambda states: calls.append(("states", list(states))),
            apply_job_order=lambda order: calls.append(("order", list(order))),
        )
        self.assertEqual(calls, [("states", [{"id": "a", "v": 2}]), ("order", ["a", "b"])])


if __name__ == "__main__":
    unittest.main()
