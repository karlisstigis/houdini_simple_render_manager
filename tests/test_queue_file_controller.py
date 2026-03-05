from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_core.queue_file_controller import QueueFileController, QueueFileControllerHooks


class QueueFileControllerTests(unittest.TestCase):
    def _make_controller(self) -> tuple[QueueFileController, dict[str, object], list[str]]:
        state: dict[str, object] = {"last_queue_path": ""}
        titles: list[str] = []
        base_dir = Path(tempfile.gettempdir()) / "hsrm_test_queue"
        controller = QueueFileController(
            "Houdini Simple Render Manager",
            QueueFileControllerHooks(
                config_get=lambda key, default=None: state.get(key, default),
                config_set=lambda key, value: state.__setitem__(key, value),
                default_queue_path=lambda: base_dir / "queue.json",
                base_dir_path=lambda: base_dir,
                queue_active=lambda: False,
                render_job_active=lambda: False,
                scan_in_progress=lambda: False,
                safe_message=lambda title, text, details=None: None,
                load_queue_from_path=lambda path: True,
                save_queue_state=lambda path=None: True,
                set_status_message=lambda text, timeout=None: None,
                set_window_title=lambda text: titles.append(text),
            ),
        )
        return controller, state, titles

    def test_current_queue_file_falls_back_to_default(self) -> None:
        controller, _state, _titles = self._make_controller()
        self.assertEqual(controller.current_queue_file_path().name, "queue.json")

    def test_set_current_queue_updates_title_state(self) -> None:
        controller, state, titles = self._make_controller()
        path = Path("C:/temp/custom_queue.json")
        controller.set_current_queue_file_path(path)
        self.assertEqual(state["last_queue_path"], str(path))
        self.assertTrue(titles)


if __name__ == "__main__":
    unittest.main()
