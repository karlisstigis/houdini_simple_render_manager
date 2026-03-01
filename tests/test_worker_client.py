from __future__ import annotations

import unittest
from pathlib import Path

from PySide6 import QtCore

from worker_client import ScanWorkerClient


class _TimeoutScanWorkerClient(ScanWorkerClient):
    def ensure_started(self) -> bool:
        return True

    def send_request(self, message_type: str, payload: dict, *, request_id: str | None = None) -> str | None:
        _ = (message_type, payload, request_id)
        return "req-timeout"


class WorkerClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QtCore.QCoreApplication.instance() or QtCore.QCoreApplication([])

    def test_scan_request_sync_times_out_with_scan_failed(self) -> None:
        client = _TimeoutScanWorkerClient(
            worker_python_path="python",
            worker_script_path=Path("scan_worker.py"),
        )
        message = client.request_sync("scan.nodes", {"hip_path": "x.hip"}, timeout_ms=1)
        self.assertIsNotNone(message)
        assert message is not None
        self.assertEqual(message["type"], "scan.failed")
        self.assertIn("Timed out", message["payload"]["message"])


if __name__ == "__main__":
    unittest.main()
