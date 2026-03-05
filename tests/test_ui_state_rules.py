from __future__ import annotations

import unittest

from ui_core.ui_state_rules import build_ui_state


class UiStateRulesTests(unittest.TestCase):
    def test_build_ui_state_running(self) -> None:
        state = build_ui_state(
            queue_active=True,
            queue_paused=False,
            render_job_active=True,
            scan_in_progress=False,
            create_job_scan_in_progress=False,
            hbatch_ok=True,
            path_sync_in_progress=False,
            experimental_chunking_enabled=True,
            chunking_checked=True,
            has_queued=True,
            can_start_selected=True,
            selected_has_log=True,
        )
        self.assertTrue(state["start_enabled"])
        self.assertEqual(state["status_message"], "Rendering...")
        self.assertFalse(state["chunk_size_enabled"])
        self.assertTrue(state["selected_has_log"])

    def test_build_ui_state_paused(self) -> None:
        state = build_ui_state(
            queue_active=True,
            queue_paused=True,
            render_job_active=False,
            scan_in_progress=False,
            create_job_scan_in_progress=False,
            hbatch_ok=True,
            path_sync_in_progress=False,
            experimental_chunking_enabled=True,
            chunking_checked=False,
            has_queued=False,
            can_start_selected=False,
            selected_has_log=False,
        )
        self.assertEqual(state["pause_text"], "Resume")
        self.assertEqual(state["status_message"], "Queue paused")

    def test_build_ui_state_path_sync_and_force_disable_chunking(self) -> None:
        state = build_ui_state(
            queue_active=False,
            queue_paused=False,
            render_job_active=False,
            scan_in_progress=False,
            create_job_scan_in_progress=False,
            hbatch_ok=True,
            path_sync_in_progress=True,
            experimental_chunking_enabled=False,
            chunking_checked=True,
            has_queued=False,
            can_start_selected=False,
            selected_has_log=False,
        )
        self.assertFalse(state["queue_file_menu_enabled"])
        self.assertTrue(state["force_disable_chunking"])
        self.assertEqual(state["status_message"], "Updating path...")


if __name__ == "__main__":
    unittest.main()
