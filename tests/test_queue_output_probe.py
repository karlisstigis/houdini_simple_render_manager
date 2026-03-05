from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_output_probe import initial_probe_path, needs_pattern_refresh, path_exists_nonempty


class QueueOutputProbeTests(unittest.TestCase):
    def test_initial_probe_path(self) -> None:
        self.assertEqual(initial_probe_path("a", "b"), "a")
        self.assertEqual(initial_probe_path("", "b"), "b")
        self.assertEqual(initial_probe_path("", ""), "")

    def test_needs_pattern_refresh(self) -> None:
        resolver = lambda path, frame: Path(path) if "$F" in path else None
        self.assertTrue(needs_pattern_refresh(probe_path="", sample_file_path="", start_frame=1, frame_path_for_frame=resolver))
        self.assertTrue(needs_pattern_refresh(probe_path="ip", sample_file_path="ip", start_frame=1, frame_path_for_frame=resolver))
        self.assertFalse(
            needs_pattern_refresh(
                probe_path="D:/img.$F4.exr",
                sample_file_path="D:/img.$F4.exr",
                start_frame=1,
                frame_path_for_frame=resolver,
            )
        )

    def test_path_exists_nonempty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "a.txt"
            path.write_text("x")
            self.assertTrue(path_exists_nonempty(path))
            empty = Path(tmpdir) / "b.txt"
            empty.write_text("")
            self.assertFalse(path_exists_nonempty(empty))
            self.assertFalse(path_exists_nonempty(Path(tmpdir) / "missing.txt"))


if __name__ == "__main__":
    unittest.main()
