from __future__ import annotations

import unittest
from pathlib import Path

from flows.queue_output_resolution_flow import maybe_refresh_probe_path, probe_pattern_resolved


class _Info:
    def __init__(self, error: str = "") -> None:
        self.error = error


class QueueOutputResolutionFlowTests(unittest.TestCase):
    def test_maybe_refresh_probe_path_skips_when_not_needed(self) -> None:
        calls: list[str] = []
        probe_path, node_not_found = maybe_refresh_probe_path(
            probe_path="D:/img.$F4.exr",
            sample_file_path="D:/img.$F4.exr",
            start_frame=1,
            hip_exists=True,
            hbatch_exists=True,
            hip_path="E:/a.hip",
            rop_path="/stage/main",
            needs_pattern_refresh_fn=lambda *_args: False,
            frame_path_for_frame_fn=lambda _path, _frame: None,
            probe_rop_info_fn=lambda _hip, _rop: calls.append("probe") or None,
            apply_rop_info_fn=lambda _info: calls.append("apply"),
            refreshed_sample_path_fn=lambda: "D:/refreshed.$F4.exr",
        )
        self.assertEqual(probe_path, "D:/img.$F4.exr")
        self.assertFalse(node_not_found)
        self.assertEqual(calls, [])

    def test_maybe_refresh_probe_path_node_not_found(self) -> None:
        probe_path, node_not_found = maybe_refresh_probe_path(
            probe_path="D:/img",
            sample_file_path="D:/img",
            start_frame=1,
            hip_exists=True,
            hbatch_exists=True,
            hip_path="E:/a.hip",
            rop_path="/stage/main",
            needs_pattern_refresh_fn=lambda *_args: True,
            frame_path_for_frame_fn=lambda _path, _frame: None,
            probe_rop_info_fn=lambda _hip, _rop: _Info(error="node_not_found"),
            apply_rop_info_fn=lambda _info: None,
            refreshed_sample_path_fn=lambda: "D:/refreshed.$F4.exr",
        )
        self.assertEqual(probe_path, "D:/img")
        self.assertTrue(node_not_found)

    def test_maybe_refresh_probe_path_applies_and_uses_refreshed_sample(self) -> None:
        calls: list[str] = []
        probe_path, node_not_found = maybe_refresh_probe_path(
            probe_path="D:/img",
            sample_file_path="D:/img",
            start_frame=1,
            hip_exists=True,
            hbatch_exists=True,
            hip_path="E:/a.hip",
            rop_path="/stage/main",
            needs_pattern_refresh_fn=lambda *_args: True,
            frame_path_for_frame_fn=lambda _path, _frame: None,
            probe_rop_info_fn=lambda _hip, _rop: _Info(error=""),
            apply_rop_info_fn=lambda _info: calls.append("apply"),
            refreshed_sample_path_fn=lambda: " D:/refreshed.$F4.exr ",
        )
        self.assertEqual(probe_path, "D:/refreshed.$F4.exr")
        self.assertFalse(node_not_found)
        self.assertEqual(calls, ["apply"])

    def test_probe_pattern_resolved(self) -> None:
        self.assertTrue(
            probe_pattern_resolved(
                probe_path="D:/img.$F4.exr",
                start_frame=1,
                frame_path_for_frame_fn=lambda _path, _frame: Path("D:/img.0001.exr"),
            )
        )
        self.assertFalse(
            probe_pattern_resolved(
                probe_path="D:/img",
                start_frame=1,
                frame_path_for_frame_fn=lambda _path, _frame: None,
            )
        )


if __name__ == "__main__":
    unittest.main()
