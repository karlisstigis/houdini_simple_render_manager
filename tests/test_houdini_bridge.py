from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from houdini_bridge import ensure_husk_hook_files


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "houdini_scripts"


class HoudiniBridgeTests(unittest.TestCase):
    def test_ensure_husk_hook_files_copies_shared_retained_usd_helper(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir)
            ensure_husk_hook_files(
                scripts_dir=SCRIPTS_DIR,
                hook_script_path_fn=lambda stem: target_dir / f"{stem}.py",
            )
            self.assertTrue((target_dir / "hsrm_retained_usd_paths.py").exists())


if __name__ == "__main__":
    unittest.main()
