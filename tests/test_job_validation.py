from __future__ import annotations

import unittest

from job_validation import (
    validate_log_file_deletion,
    validate_logs_folder_access,
    validate_output_folder_open,
    validate_preview_launch,
    validate_render_missing_inputs,
    validate_render_missing_probe_path,
    validate_resolved_frame_range_for_resume,
    validate_resume_from_output_inputs,
    validate_resume_probe_path,
)


class JobValidationTests(unittest.TestCase):
    def test_resume_validation_blocks_strict_range(self) -> None:
        decision = validate_resume_from_output_inputs(strict_frame_range=True)
        self.assertFalse(decision.valid)
        self.assertIn("Strict frame range", decision.message)

    def test_resume_resolved_range_validation(self) -> None:
        self.assertFalse(validate_resolved_frame_range_for_resume(None, offline=False).valid)
        self.assertTrue(validate_resolved_frame_range_for_resume((1001, 1010, 1), offline=False).valid)

    def test_probe_path_validations(self) -> None:
        self.assertFalse(validate_resume_probe_path(probe_path="", pattern_resolved=False).valid)
        self.assertFalse(validate_render_missing_probe_path(probe_path="ip", pattern_resolved=False).valid)
        self.assertTrue(validate_render_missing_probe_path(probe_path="X:/render/test.$F4.exr", pattern_resolved=True).valid)

    def test_render_missing_inputs_validation(self) -> None:
        self.assertFalse(validate_render_missing_inputs(None, offline=False).valid)
        self.assertTrue(validate_render_missing_inputs((1001, 1010, 1), offline=False).valid)

    def test_preview_and_folder_validations(self) -> None:
        self.assertFalse(validate_preview_launch(preview_path_exists=False, player_path_set=True, player_exists=True).valid)
        self.assertFalse(validate_logs_folder_access(folder_ready=False, create_failed=True).valid)
        self.assertFalse(validate_log_file_deletion(logs_busy=True, has_logs=True).valid)
        self.assertFalse(validate_output_folder_open(folder_exists=False).valid)


if __name__ == "__main__":
    unittest.main()
