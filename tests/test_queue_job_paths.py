from __future__ import annotations

import unittest
from pathlib import Path

from queue_core.queue_job_paths import (
    configured_retained_usd_folder_preview,
    job_file_name_from_path,
    job_rop_name_from_path,
    safe_usd_folder_name,
)
from queue_core.queue_models import UsdOutputDirectoryMode


class QueueJobPathsTests(unittest.TestCase):
    def test_job_file_name_from_path(self) -> None:
        self.assertEqual(job_file_name_from_path(""), "-")
        self.assertEqual(job_file_name_from_path("E:/show/scene/test.hip"), "test.hip")

    def test_job_rop_name_from_path(self) -> None:
        self.assertEqual(job_rop_name_from_path(""), "-")
        self.assertEqual(job_rop_name_from_path("/stage/CamTop"), "CamTop")
        self.assertEqual(job_rop_name_from_path("/stage/CamTop/"), "CamTop")

    def test_safe_usd_folder_name(self) -> None:
        self.assertEqual(safe_usd_folder_name("Cam Top"), "Cam_Top")
        self.assertEqual(safe_usd_folder_name("  "), "rop")

    def test_configured_retained_usd_folder_preview_default_temp(self) -> None:
        self.assertEqual(
            configured_retained_usd_folder_preview(
                hip_path="E:/show/shot/test.hip",
                rop_path="/stage/CamTop",
                mode=UsdOutputDirectoryMode.DEFAULT_TEMP,
                custom_path="",
            ),
            "",
        )

    def test_configured_retained_usd_folder_preview_project_path(self) -> None:
        preview = configured_retained_usd_folder_preview(
            hip_path="E:/show/shot/test.hip",
            rop_path="/stage/Cam Top",
            mode=UsdOutputDirectoryMode.PROJECT_PATH,
            custom_path="",
        )
        self.assertEqual(preview, str(Path("E:/show/shot") / "usd_renders" / "test" / "Cam_Top_$RENDERID"))

    def test_configured_retained_usd_folder_preview_custom_path(self) -> None:
        preview = configured_retained_usd_folder_preview(
            hip_path="E:/show/shot/test.hip",
            rop_path="/stage/CamTop",
            mode=UsdOutputDirectoryMode.CUSTOM_PATH,
            custom_path="D:/cache/usd",
        )
        self.assertEqual(preview, str(Path("D:/cache/usd") / "CamTop_$RENDERID"))


if __name__ == "__main__":
    unittest.main()
