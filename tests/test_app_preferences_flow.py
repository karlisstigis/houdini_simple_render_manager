from __future__ import annotations

import unittest

from flows.app_preferences_flow import (
    dialog_device_defaults,
    dialog_experimental_flags,
    dialog_runtime_defaults,
    parse_preferences_payload,
)
from queue_models import DeviceOverrideMode, UsdOutputDirectoryMode


class AppPreferencesFlowTests(unittest.TestCase):
    def test_dialog_payload_helpers(self) -> None:
        runtime = dialog_runtime_defaults(
            chunking_enabled=True,
            chunk_size=12,
            retry_count=3,
            retry_delay=8,
        )
        experimental = dialog_experimental_flags(chunking_enabled=False)
        device = dialog_device_defaults(
            mode=DeviceOverrideMode.SPECIFIC_GPUS,
            selection="0,1",
            retain_built_usd=True,
            usd_output_directory_mode=UsdOutputDirectoryMode.CUSTOM_PATH,
            usd_output_directory_custom_path="E:/usd",
        )
        self.assertEqual(runtime["chunk_size"], 12)
        self.assertFalse(experimental["chunking"])
        self.assertEqual(device["mode"], DeviceOverrideMode.SPECIFIC_GPUS.value)
        self.assertEqual(device["usd_output_directory_mode"], UsdOutputDirectoryMode.CUSTOM_PATH.value)

    def test_parse_preferences_payload_normalizes_and_clamps(self) -> None:
        parsed = parse_preferences_payload(
            {
                "hbatch_path": " C:/H/bin/hbatch.exe ",
                "player_path": " C:/Player/player.exe ",
                "theme": {"panel_gap": 1, "background": "#123456"},
                "runtime_defaults": {
                    "chunking_enabled": True,
                    "chunk_size": "0",
                    "retry_count": "-1",
                    "retry_delay": "5",
                },
                "experimental_flags": {"chunking": True},
                "device_defaults": {
                    "mode": "specific_gpus",
                    "selection": " cpu,0,foo,1 ",
                    "retain_built_usd": 1,
                    "usd_output_directory_mode": "custom_path",
                    "usd_output_directory_custom_path": " E:/usd/custom ",
                },
            }
        )
        self.assertEqual(parsed["hbatch_path"], "C:/H/bin/hbatch.exe")
        self.assertEqual(parsed["player_path"], "C:/Player/player.exe")
        self.assertTrue(parsed["experimental_chunking_enabled"])
        self.assertEqual(parsed["runtime_defaults"], (True, 1, 0, 5))
        self.assertIsNotNone(parsed["theme"])
        self.assertEqual(parsed["theme"]["panel_gap"], 2)
        self.assertEqual(parsed["theme"]["background"], "#123456")

        mode, selection, retain_built_usd, usd_mode, custom_path = parsed["device_defaults"]
        self.assertEqual(mode, DeviceOverrideMode.SPECIFIC_GPUS)
        self.assertEqual(selection, "cpu,0,1")
        self.assertTrue(retain_built_usd)
        self.assertEqual(usd_mode, UsdOutputDirectoryMode.CUSTOM_PATH)
        self.assertEqual(custom_path, "E:/usd/custom")

    def test_parse_preferences_payload_handles_invalid_sections(self) -> None:
        parsed = parse_preferences_payload(
            {
                "theme": [],
                "runtime_defaults": "bad",
                "device_defaults": "bad",
                "experimental_flags": "bad",
            }
        )
        self.assertIsNone(parsed["theme"])
        self.assertIsNone(parsed["runtime_defaults"])
        self.assertIsNone(parsed["device_defaults"])
        self.assertIsNone(parsed["experimental_chunking_enabled"])


if __name__ == "__main__":
    unittest.main()
