from __future__ import annotations

import unittest
from pathlib import Path

from queue_core.queue_output_paths import (
    frame_sequence_path_for_frame,
    normalize_output_display_path,
    output_folder_from_value,
)


class QueueOutputPathsTests(unittest.TestCase):
    def test_frame_sequence_path_for_frame_replaces_dollar_token(self) -> None:
        result = frame_sequence_path_for_frame("D:/render/beauty.$F4.exr", 12)
        self.assertEqual(result, Path("D:/render/beauty.0012.exr"))

    def test_frame_sequence_path_for_frame_replaces_brace_token_default_padding(self) -> None:
        result = frame_sequence_path_for_frame("D:/render/beauty.${F}.exr", 7)
        self.assertEqual(result, Path("D:/render/beauty.7.exr"))

    def test_frame_sequence_path_for_frame_rejects_unknown_tokenized_format(self) -> None:
        self.assertIsNone(frame_sequence_path_for_frame("D:/render/beauty.%04d.exr", 10))

    def test_frame_sequence_path_for_frame_replaces_last_numeric_sequence(self) -> None:
        result = frame_sequence_path_for_frame("D:/render/beauty.0100.exr", 98)
        self.assertEqual(result, Path("D:/render/beauty.0098.exr"))

    def test_frame_sequence_path_for_frame_handles_negative_frame(self) -> None:
        result = frame_sequence_path_for_frame("D:/render/beauty.$F3.exr", -5)
        self.assertEqual(result, Path("D:/render/beauty.-005.exr"))

    def test_frame_sequence_path_for_frame_ignores_ip(self) -> None:
        self.assertIsNone(frame_sequence_path_for_frame("ip", 1001))

    def test_normalize_output_display_path(self) -> None:
        self.assertEqual(normalize_output_display_path(""), "")
        self.assertEqual(normalize_output_display_path("ip"), "ip")
        self.assertEqual(normalize_output_display_path("D:/render/beauty.$F4.exr"), "D:\\render")
        self.assertEqual(normalize_output_display_path("D:/render/folder"), "D:\\render\\folder")

    def test_output_folder_from_value(self) -> None:
        self.assertIsNone(output_folder_from_value(""))
        self.assertIsNone(output_folder_from_value("ip"))
        self.assertEqual(output_folder_from_value("D:/render/beauty.$F4.exr"), Path("D:/render"))
        self.assertEqual(output_folder_from_value("D:/render/folder"), Path("D:/render/folder"))


if __name__ == "__main__":
    unittest.main()
