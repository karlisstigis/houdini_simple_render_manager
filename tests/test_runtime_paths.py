from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

from app_core.runtime_paths import bundle_root, bundled_path, executable_path, executable_root, source_root


class RuntimePathsTests(unittest.TestCase):
    def test_source_mode_uses_main_file_parent(self) -> None:
        main_file = Path("E:/repo/houdini_simple_render_manager.py")
        with mock.patch.object(sys, "frozen", False, create=True):
            self.assertEqual(source_root(main_file), Path("E:/repo"))
            self.assertEqual(bundle_root(main_file), Path("E:/repo"))
            self.assertEqual(executable_root(main_file), Path("E:/repo"))
            self.assertEqual(bundled_path(main_file, "assets", "x.svg"), Path("E:/repo/assets/x.svg"))
            self.assertEqual(executable_path(main_file, "scan_worker.py"), Path("E:/repo/scan_worker.py"))

    def test_frozen_mode_splits_bundle_and_executable_roots(self) -> None:
        main_file = Path("E:/repo/houdini_simple_render_manager.py")
        with mock.patch.object(sys, "frozen", True, create=True):
            with mock.patch.object(sys, "_MEIPASS", "E:/dist/HoudiniSimpleRenderManager/_internal", create=True):
                with mock.patch.object(sys, "executable", "E:/dist/HoudiniSimpleRenderManager/HoudiniSimpleRenderManager.exe"):
                    self.assertEqual(bundle_root(main_file), Path("E:/dist/HoudiniSimpleRenderManager/_internal"))
                    self.assertEqual(executable_root(main_file), Path("E:/dist/HoudiniSimpleRenderManager"))
                    self.assertEqual(
                        bundled_path(main_file, "houdini_scripts"),
                        Path("E:/dist/HoudiniSimpleRenderManager/_internal/houdini_scripts"),
                    )
                    self.assertEqual(
                        executable_path(main_file, "scan_worker.exe"),
                        Path("E:/dist/HoudiniSimpleRenderManager/scan_worker.exe"),
                    )


if __name__ == "__main__":
    unittest.main()
