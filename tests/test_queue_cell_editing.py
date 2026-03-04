from __future__ import annotations

import unittest

from queue_cell_editing import apply_queue_cell_edit
from queue_models import FrameHandlingMode, RenderJob


class _QueueTableModelStub:
    def __init__(self) -> None:
        self.refreshed_ids: list[list[str]] = []

    def refresh_jobs_by_id(self, ids: list[str]) -> None:
        self.refreshed_ids.append(list(ids))


class _QueueTableStub:
    def inline_edit_target_rows(self) -> list[int]:
        return [0]


class _WindowStub:
    def __init__(self) -> None:
        self.jobs = [RenderJob("E:/shot/test.hip", "/stage/usdrender", "override")]
        self.queue_table = _QueueTableStub()
        self.queue_table_model = _QueueTableModelStub()
        self.history: list[dict] = []
        self.deferred: list[list[str]] = []
        self.refreshed = False

    def _is_job_path_sync_locked(self, _job) -> bool:
        return False

    def _selected_rows(self) -> list[int]:
        return [0]

    def _job_states_for_ids(self, ids: list[str]) -> list[dict]:
        return [{"id": job.id} for job in self.jobs if job.id in ids]

    def _push_history_command(self, payload: dict) -> None:
        self.history.append(payload)

    def _defer_save_and_refresh_queue(self, ids: list[str]) -> None:
        self.deferred.append(list(ids))

    def _refresh_queue_preserve_selection(self) -> None:
        self.refreshed = True

    def _is_active_job(self, _job) -> bool:
        return False

    def _affected_job_ids_for_hip_path_change(self, _old_hip: str) -> list[str]:
        return [self.jobs[0].id]

    def _affected_job_ids_for_rop_path_change(self, _old_hip: str, _old_rop: str) -> list[str]:
        return [self.jobs[0].id]

    def _apply_hip_path_change_immediately(self, _old: str, _new: str) -> list[str]:
        return [self.jobs[0].id]

    def _apply_rop_path_change_immediately(self, _old_hip: str, _old_rop: str, _new: str) -> list[str]:
        return [self.jobs[0].id]

    def _selection_ids_for_refresh(self, changed_ids: list[str]) -> list[str]:
        return list(changed_ids)

    def _defer_finalize_path_change(self, **_kwargs) -> None:
        return None

    def _queue_model_display_text(self, _row: int, _col: int) -> str:
        return "1-10" if _col == 3 else "1"

    def _queue_edit_frame_text_for_job(self, _job) -> str:
        return "1-10"

    def _queue_edit_step_text_for_job(self, _job) -> str:
        return "1"


class QueueCellEditingTests(unittest.TestCase):
    def test_invalid_column_rejected(self) -> None:
        window = _WindowStub()
        messages: list[tuple[str, str]] = []
        result = apply_queue_cell_edit(
            window,
            row=0,
            col=99,
            text="x",
            show_message=lambda title, message, details=None: messages.append((title, message)),
        )
        self.assertFalse(result)
        self.assertEqual(messages, [])

    def test_frame_handling_edit_updates_job_and_history(self) -> None:
        window = _WindowStub()
        job = window.jobs[0]
        self.assertIs(job.spec.frame_handling_mode, FrameHandlingMode.RENDER_MISSING)

        result = apply_queue_cell_edit(
            window,
            row=0,
            col=5,
            text=FrameHandlingMode.OVERWRITE.label(),
            show_message=lambda title, message, details=None: None,
        )

        self.assertTrue(result)
        self.assertIs(job.spec.frame_handling_mode, FrameHandlingMode.OVERWRITE)
        self.assertEqual(len(window.history), 1)
        self.assertEqual(window.queue_table_model.refreshed_ids, [[job.id]])


if __name__ == "__main__":
    unittest.main()
