from __future__ import annotations

import unittest

from queue_core.queue_refresh_defer import (
    next_pending_refresh_action,
    pending_refresh_args,
    should_defer_queue_refresh,
)


class _Editable:
    pass


class _Other:
    pass


class QueueRefreshDeferTests(unittest.TestCase):
    def test_should_defer_queue_refresh(self) -> None:
        self.assertFalse(
            should_defer_queue_refresh(
                focus=None,
                queue_is_editing=False,
                focus_in_queue=False,
                focus_in_add_panel=False,
                queue_editable_types=(_Editable,),
                add_panel_editable_types=(_Editable,),
            )
        )
        self.assertTrue(
            should_defer_queue_refresh(
                focus=_Other(),
                queue_is_editing=True,
                focus_in_queue=False,
                focus_in_add_panel=False,
                queue_editable_types=(_Editable,),
                add_panel_editable_types=(_Editable,),
            )
        )
        self.assertTrue(
            should_defer_queue_refresh(
                focus=_Editable(),
                queue_is_editing=False,
                focus_in_queue=True,
                focus_in_add_panel=False,
                queue_editable_types=(_Editable,),
                add_panel_editable_types=(_Editable,),
            )
        )
        self.assertFalse(
            should_defer_queue_refresh(
                focus=_Other(),
                queue_is_editing=False,
                focus_in_queue=True,
                focus_in_add_panel=False,
                queue_editable_types=(_Editable,),
                add_panel_editable_types=(_Editable,),
            )
        )

    def test_pending_refresh_args(self) -> None:
        payload = pending_refresh_args(select_row=2, select_job_id="job-1", select_job_ids=["a", "b"])
        self.assertEqual(
            payload,
            {"select_row": 2, "select_job_id": "job-1", "select_job_ids": ["a", "b"]},
        )
        payload2 = pending_refresh_args(select_job_ids=[])
        self.assertEqual(payload2["select_job_ids"], None)

    def test_next_pending_refresh_action(self) -> None:
        args, reschedule = next_pending_refresh_action(None, should_defer=False)
        self.assertIsNone(args)
        self.assertFalse(reschedule)

        args2, reschedule2 = next_pending_refresh_action({"select_job_id": "a"}, should_defer=True)
        self.assertIsNone(args2)
        self.assertTrue(reschedule2)

        source = {"select_job_id": "a"}
        args3, reschedule3 = next_pending_refresh_action(source, should_defer=False)
        self.assertEqual(args3, source)
        self.assertFalse(reschedule3)
        self.assertIsNot(args3, source)


if __name__ == "__main__":
    unittest.main()
