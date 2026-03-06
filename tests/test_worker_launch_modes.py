from __future__ import annotations

import unittest
from pathlib import Path

from worker_core.worker_client import ScanWorkerClient


class WorkerLaunchModeTests(unittest.TestCase):
    def test_python_script_mode_uses_python_and_script_argument(self) -> None:
        client = ScanWorkerClient(worker_python_path="python", worker_script_path=Path("scan_worker.py"))
        self.assertEqual(client._launch_command(), ("python", ["scan_worker.py"]))

    def test_executable_mode_launches_worker_exe_directly(self) -> None:
        client = ScanWorkerClient(worker_python_path="", worker_script_path=Path("scan_worker.exe"))
        self.assertEqual(client._launch_command(), ("scan_worker.exe", []))


if __name__ == "__main__":
    unittest.main()
