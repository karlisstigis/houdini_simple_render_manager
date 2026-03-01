from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from atomic_io import read_json_file, write_json_atomic
from worker_protocol import decode_message, encode_message


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AtomicIoTests(unittest.TestCase):
    def test_atomic_write_creates_backup_on_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.json"
            write_json_atomic(path, {"a": 1})
            write_json_atomic(path, {"a": 2})
            backup_path = Path(tmpdir) / "queue.json.bak"
            self.assertTrue(path.exists())
            self.assertTrue(backup_path.exists())
            self.assertEqual(read_json_file(path)["a"], 2)
            self.assertEqual(read_json_file(backup_path)["a"], 1)


class WorkerProtocolTests(unittest.TestCase):
    def test_encode_decode_round_trip(self) -> None:
        encoded = encode_message("scan.nodes", "req1", {"hip_path": "x.hip"})
        decoded = decode_message(encoded.decode("utf-8"))
        self.assertIsNotNone(decoded)
        assert decoded is not None
        self.assertEqual(decoded["type"], "scan.nodes")
        self.assertEqual(decoded["request_id"], "req1")
        self.assertEqual(decoded["payload"]["hip_path"], "x.hip")


class WorkerProcessFaultTests(unittest.TestCase):
    def _run_worker(self, script_name: str, input_lines: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(PROJECT_ROOT / script_name)],
            input="".join(input_lines),
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )

    def test_render_worker_rejects_unknown_message(self) -> None:
        result = self._run_worker("render_worker.py", ['{"type":"ping","request_id":"r1","payload":{}}\n'])
        self.assertEqual(result.returncode, 0)
        self.assertIn('"type": "worker.error"', result.stdout)

    def test_scan_worker_rejects_unknown_message(self) -> None:
        result = self._run_worker("scan_worker.py", ['{"type":"ping","request_id":"s1","payload":{}}\n'])
        self.assertEqual(result.returncode, 0)
        self.assertIn('"type": "worker.error"', result.stdout)

    def test_render_worker_invalid_start_payload_reports_crash(self) -> None:
        result = self._run_worker("render_worker.py", ['{"type":"render.start","request_id":"r2","payload":{}}\n'])
        self.assertEqual(result.returncode, 0)
        self.assertIn('"type": "render.crashed"', result.stdout)
        self.assertIn('"process_error": "invalid_payload"', result.stdout)


if __name__ == "__main__":
    unittest.main()
