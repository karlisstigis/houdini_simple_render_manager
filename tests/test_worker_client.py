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


class _FakeProcess(QtCore.QObject):
    def __init__(self) -> None:
        super().__init__()
        self._state = QtCore.QProcess.ProcessState.Running
        self.killed = False

    def state(self) -> QtCore.QProcess.ProcessState:
        return self._state

    def kill(self) -> None:
        self.killed = True
        self._state = QtCore.QProcess.ProcessState.NotRunning

    def waitForFinished(self, timeout_ms: int) -> bool:
        _ = timeout_ms
        self._state = QtCore.QProcess.ProcessState.NotRunning
        return True


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

    def test_worker_hang_emits_failure_and_kills_process(self) -> None:
        client = ScanWorkerClient(
            worker_python_path="python",
            worker_script_path=Path("scan_worker.py"),
        )
        fake_process = _FakeProcess()
        client._process = fake_process
        client._set_active_request_id("req-hung")
        client._last_activity_monotonic = 0.0
        client._heartbeat_timeout_sec = 0.0
        failures: list[str] = []
        client.worker_failed.connect(failures.append)

        client._check_health()

        self.assertFalse(client.is_busy())
        self.assertTrue(fake_process.killed)
        self.assertEqual(len(failures), 1)
        self.assertIn("unresponsive", failures[0].lower())


if __name__ == "__main__":
    unittest.main()
