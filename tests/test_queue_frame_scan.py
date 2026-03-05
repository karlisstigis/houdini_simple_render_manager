from __future__ import annotations

import unittest
from pathlib import Path

from queue_frame_scan import (
    compress_missing_frames_to_runs,
    first_missing_frame_and_contiguous_done,
    missing_frame_runs_and_existing_count,
)


class QueueFrameScanTests(unittest.TestCase):
    def test_first_missing_frame_and_contiguous_done(self) -> None:
        existing = {Path("f1"), Path("f2")}
        result = first_missing_frame_and_contiguous_done(
            start_frame=1,
            end_frame=4,
            step=1,
            path_for_frame=lambda frame: Path(f"f{frame}"),
            exists_nonempty=lambda path: path in existing,
        )
        self.assertEqual(result, (3, 2, 4))

    def test_first_missing_frame_and_contiguous_done_complete(self) -> None:
        existing = {Path("f1"), Path("f2")}
        result = first_missing_frame_and_contiguous_done(
            start_frame=1,
            end_frame=2,
            step=1,
            path_for_frame=lambda frame: Path(f"f{frame}"),
            exists_nonempty=lambda path: path in existing,
        )
        self.assertEqual(result, (None, 2, 2))

    def test_first_missing_frame_and_contiguous_done_path_failure(self) -> None:
        result = first_missing_frame_and_contiguous_done(
            start_frame=1,
            end_frame=2,
            step=1,
            path_for_frame=lambda frame: None if frame == 2 else Path("ok"),
            exists_nonempty=lambda _path: True,
        )
        self.assertIsNone(result)

    def test_compress_missing_frames_to_runs(self) -> None:
        runs = compress_missing_frames_to_runs([1, 2, 5, 6, 9], step=1)
        self.assertEqual(runs, [(1, 2, 1), (5, 6, 1), (9, 9, 1)])

    def test_missing_frame_runs_and_existing_count(self) -> None:
        existing = {Path("f1"), Path("f3"), Path("f7")}
        result = missing_frame_runs_and_existing_count(
            start_frame=1,
            end_frame=7,
            step=2,
            path_for_frame=lambda frame: Path(f"f{frame}"),
            exists_nonempty=lambda path: path in existing,
        )
        self.assertEqual(result, ([(5, 5, 2)], 3))

    def test_missing_frame_runs_and_existing_count_path_failure(self) -> None:
        result = missing_frame_runs_and_existing_count(
            start_frame=1,
            end_frame=3,
            step=1,
            path_for_frame=lambda frame: None if frame == 2 else Path("ok"),
            exists_nonempty=lambda _path: True,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
