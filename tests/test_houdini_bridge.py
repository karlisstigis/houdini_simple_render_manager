from __future__ import annotations

import shutil
import unittest
from uuid import uuid4
from pathlib import Path

from houdini_core.houdini_bridge import ensure_husk_hook_files


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "houdini_scripts"
TEST_TEMP_ROOT = PROJECT_ROOT / ".tmp_test"


class HoudiniBridgeTests(unittest.TestCase):
    def test_ensure_husk_hook_files_copies_shared_retained_usd_helper(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        target_dir = TEST_TEMP_ROOT / f"houdini_bridge_{uuid4().hex}"
        target_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, target_dir, True)
        ensure_husk_hook_files(
            scripts_dir=SCRIPTS_DIR,
            hook_script_path_fn=lambda stem: target_dir / f"{stem}.py",
        )
        self.assertTrue((target_dir / "hsrm_retained_usd_paths.py").exists())


if __name__ == "__main__":
    unittest.main()
