from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from preview_paths import resolve_job_preview_path


class PreviewPathsTests(unittest.TestCase):
    def test_returns_none_for_empty_or_ip(self) -> None:
        self.assertIsNone(
            resolve_job_preview_path(
                candidate="",
                resolved_range=None,
                frame_path_for_frame=lambda _path, _frame: None,
            )
        )
        self.assertIsNone(
            resolve_job_preview_path(
                candidate="ip",
                resolved_range=(1, 3, 1),
                frame_path_for_frame=lambda _path, _frame: None,
            )
        )

    def test_returns_direct_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "frame.exr"
            path.write_text("x", encoding="utf-8")
            resolved = resolve_job_preview_path(
                candidate=str(path),
                resolved_range=(1, 3, 1),
                frame_path_for_frame=lambda _sample, _frame: None,
            )
            self.assertEqual(resolved, path)

    def test_prefers_existing_sequence_frame(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            existing = Path(tmpdir) / "image.0002.exr"
            existing.write_text("x", encoding="utf-8")

            def frame_path(_sample: str, frame: int) -> Path:
                return Path(tmpdir) / f"image.{frame:04d}.exr"

            resolved = resolve_job_preview_path(
                candidate="E:/render/image.$F4.exr",
                resolved_range=(1, 3, 1),
                frame_path_for_frame=frame_path,
            )
            self.assertEqual(resolved, existing)

    def test_falls_back_to_start_frame_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            expected = Path(tmpdir) / "image.0001.exr"
            resolved = resolve_job_preview_path(
                candidate="E:/render/image.$F4.exr",
                resolved_range=(1, 3, 1),
                frame_path_for_frame=lambda _sample, frame: Path(tmpdir) / f"image.{frame:04d}.exr",
            )
            self.assertEqual(resolved, expected)

    def test_returns_none_for_path_without_suffix(self) -> None:
        resolved = resolve_job_preview_path(
            candidate="E:/render/output",
            resolved_range=None,
            frame_path_for_frame=lambda _sample, _frame: None,
        )
        self.assertIsNone(resolved)


if __name__ == "__main__":
    unittest.main()
