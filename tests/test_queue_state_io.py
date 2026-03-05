from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_state_io import load_queue_state, save_queue_state


class QueueStateIoTests(unittest.TestCase):
    def test_load_queue_state_filters_invalid_entries(self) -> None:
        def _load_payload(_path: Path) -> dict:
            return {
                "jobs": [{"id": "job-a"}, "bad", {"id": "job-b"}],
                "queue_view": {"column_widths": {"0": 200}},
                "active_job_id": "job-a",
            }

        def _job_from_dict(item: dict, active_job_id: str):  # type: ignore[no-untyped-def]
            return {"id": item.get("id"), "active": item.get("id") == active_job_id}

        jobs, queue_view, active_job_id = load_queue_state(
            Path("E:/queue.json"),
            load_queue_payload_fn=_load_payload,
            job_from_persisted_dict_fn=_job_from_dict,
        )
        self.assertEqual(active_job_id, "job-a")
        self.assertEqual(queue_view, {"column_widths": {"0": 200}})
        self.assertEqual(jobs, [{"id": "job-a", "active": True}, {"id": "job-b", "active": False}])

    def test_save_queue_state_writes_to_override_path(self) -> None:
        calls: list[dict] = []

        def _save_payload(path: Path, **kwargs):  # type: ignore[no-untyped-def]
            calls.append({"path": path, **kwargs})

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            current = root / "queue.json"
            override = root / "nested" / "queue_alt.json"
            target = save_queue_state(
                current_queue_path=current,
                path_override=override,
                jobs=[{"id": "job-a"}],
                queue_view={"hidden_columns": []},
                active_job_id="job-a",
                save_queue_payload_fn=_save_payload,
            )
            self.assertEqual(target, override)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["path"], override)
            self.assertEqual(calls[0]["active_job_id"], "job-a")


if __name__ == "__main__":
    unittest.main()
