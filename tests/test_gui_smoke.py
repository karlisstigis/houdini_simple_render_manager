from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class GuiSmokeTests(unittest.TestCase):
    def test_gui_smoke_script(self) -> None:
        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "gui_smoke.py")],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
