from __future__ import annotations

import unittest

from houdini_simple_render_manager import MainWindow


class NotificationParserTests(unittest.TestCase):
    def test_gpu_out_of_memory_is_classified(self) -> None:
        summary = MainWindow._notification_summary_for_line("stderr", "CUDA error: out of memory")
        self.assertEqual(summary, ("Render failed: GPU out of memory.", "error"))

    def test_gpu_device_lost_is_classified(self) -> None:
        summary = MainWindow._notification_summary_for_line("stderr", "VK_ERROR_DEVICE_LOST while rendering")
        self.assertEqual(summary, ("Render failed: GPU device lost or driver reset.", "error"))

    def test_gpu_missing_is_classified(self) -> None:
        summary = MainWindow._notification_summary_for_line("stderr", "Failed to initialize CUDA backend")
        self.assertEqual(summary, ("Render failed: no compatible GPU device available.", "error"))

    def test_system_memory_is_classified(self) -> None:
        summary = MainWindow._notification_summary_for_line("stderr", "std::bad_alloc")
        self.assertEqual(summary, ("Render failed: system memory exhausted.", "error"))


if __name__ == "__main__":
    unittest.main()
