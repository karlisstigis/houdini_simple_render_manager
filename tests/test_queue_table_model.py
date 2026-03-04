from __future__ import annotations

import unittest

from PySide6 import QtCore, QtGui, QtWidgets

from queue_models import JobStatus, RenderJob
from queue_table_model import (
    DISPLAY_STATUS_ROLE,
    EDITABLE_ROLE,
    OVERRIDE_RANGE_ROLE,
    OVERRIDE_STEP_ROLE,
    PROGRESS_BUILD_ROLE,
    PROGRESS_RENDER_ROLE,
    QueueTableModel,
    QueueTableModelHooks,
)


class QueueTableModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def _build_model(self, jobs: list[RenderJob], edit_hook=None) -> QueueTableModel:
        return QueueTableModel(
            QueueTableModelHooks(
                jobs_provider=lambda: jobs,
                is_active_job=lambda job: job.runtime.status == JobStatus.RUNNING,
                job_phase_display=lambda job: job.view.phase_text,
                job_usd_status_display=lambda job: "Build",
                job_usd_status_tooltip=lambda job: "USD tooltip",
                job_time_remaining_display=lambda job: "remain",
                job_frame_display=lambda job: "frame",
                job_started_time_display=lambda job: "started",
                job_end_time_display=lambda job: "done",
                job_total_time_display=lambda job: "total",
                queue_progress_split_values=lambda job: (25, 75),
                edit_job_column=edit_hook or (lambda row, column, text: True),
                can_edit_job_column=lambda job, column: column in {0, 1, 2, 3, 4, 5} and job.runtime.status != JobStatus.RUNNING and not (column in {3, 4} and job.spec.strict_frame_range),
                is_job_path_sync_locked=lambda job: False,
                row_style_payload=lambda job, row: {
                    "background": QtGui.QBrush(QtGui.QColor("#111111")) if job.runtime.status == JobStatus.RUNNING else None,
                    "foreground": QtGui.QBrush(QtGui.QColor("#ffffff")) if job.runtime.status == JobStatus.RUNNING else None,
                },
                theme_icon_path=lambda key: "",
            )
        )

    def test_headers_and_display_values(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "use_rop", name="Shot A")
        job.view.percent_text = "42%"
        model = self._build_model([job])
        self.assertEqual(model.rowCount(), 1)
        self.assertEqual(model.columnCount(), 18)
        self.assertEqual(model.headerData(0, QtCore.Qt.Orientation.Horizontal), "Name")
        self.assertEqual(model.headerData(9, QtCore.Qt.Orientation.Horizontal), "USD")
        self.assertEqual(model.data(model.index(0, 0), QtCore.Qt.ItemDataRole.DisplayRole), "Shot A")
        self.assertEqual(model.data(model.index(0, 1), QtCore.Qt.ItemDataRole.DisplayRole), "E:/a.hip")
        self.assertEqual(model.data(model.index(0, 7), QtCore.Qt.ItemDataRole.DisplayRole), "42%")
        self.assertEqual(model.data(model.index(0, QueueTableModel.USD_COLUMN), QtCore.Qt.ItemDataRole.DisplayRole), "Build")
        self.assertEqual(model.data(model.index(0, QueueTableModel.USD_COLUMN), QtCore.Qt.ItemDataRole.ToolTipRole), "USD tooltip")

    def test_roles_and_flags(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "use_rop", status=JobStatus.RUNNING)
        model = self._build_model([job])
        idx = model.index(0, 7)
        self.assertEqual(model.data(idx, DISPLAY_STATUS_ROLE), JobStatus.RUNNING.value)
        self.assertEqual(model.data(idx, PROGRESS_BUILD_ROLE), 25)
        self.assertEqual(model.data(idx, PROGRESS_RENDER_ROLE), 75)
        self.assertFalse(bool(model.data(model.index(0, 0), EDITABLE_ROLE)))
        self.assertFalse(bool(model.flags(model.index(0, 0)) & QtCore.Qt.ItemFlag.ItemIsEditable))

    def test_strict_range_columns_are_not_editable(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "use_rop")
        job.spec.strict_frame_range = True
        model = self._build_model([job])
        self.assertFalse(bool(model.data(model.index(0, 3), EDITABLE_ROLE)))
        self.assertFalse(bool(model.flags(model.index(0, 3)) & QtCore.Qt.ItemFlag.ItemIsEditable))

    def test_set_data_calls_edit_hook(self) -> None:
        calls: list[tuple[int, int, str]] = []
        job = RenderJob("E:/a.hip", "/out/karma1", "use_rop")
        model = self._build_model([job], edit_hook=lambda row, column, text: calls.append((row, column, text)) or True)
        result = model.setData(model.index(0, 0), "New Name")
        self.assertTrue(result)
        self.assertEqual(calls, [(0, 0, "New Name")])

    def test_override_flags_use_cached_rop_defaults_when_runtime_range_is_unset(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "override", start_frame=105, end_frame=110, step=2)
        job.runtime.rop_default_start_frame = 100
        job.runtime.rop_default_end_frame = 110
        job.runtime.rop_default_step = 1
        job.runtime.runtime_start_frame = None
        job.runtime.runtime_end_frame = None
        job.runtime.runtime_step = None
        model = self._build_model([job])
        self.assertTrue(bool(model.data(model.index(0, QueueTableModel.FRAME_RANGE_COLUMN), OVERRIDE_RANGE_ROLE)))
        self.assertTrue(bool(model.data(model.index(0, QueueTableModel.STEP_COLUMN), OVERRIDE_STEP_ROLE)))

    def test_override_flags_clear_when_override_matches_cached_rop_defaults(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "override", start_frame=100, end_frame=110, step=1)
        job.runtime.rop_default_start_frame = 100
        job.runtime.rop_default_end_frame = 110
        job.runtime.rop_default_step = 1
        job.runtime.runtime_start_frame = 1
        job.runtime.runtime_end_frame = 2
        job.runtime.runtime_step = 3
        model = self._build_model([job])
        self.assertFalse(bool(model.data(model.index(0, QueueTableModel.FRAME_RANGE_COLUMN), OVERRIDE_RANGE_ROLE)))
        self.assertFalse(bool(model.data(model.index(0, QueueTableModel.STEP_COLUMN), OVERRIDE_STEP_ROLE)))

    def test_override_flags_do_not_fall_back_to_runtime_values(self) -> None:
        job = RenderJob("E:/a.hip", "/out/karma1", "override", start_frame=100, end_frame=110, step=1)
        job.runtime.runtime_start_frame = 100
        job.runtime.runtime_end_frame = 110
        job.runtime.runtime_step = 1
        job.runtime.rop_default_start_frame = None
        job.runtime.rop_default_end_frame = None
        job.runtime.rop_default_step = None
        model = self._build_model([job])
        self.assertTrue(bool(model.data(model.index(0, QueueTableModel.FRAME_RANGE_COLUMN), OVERRIDE_RANGE_ROLE)))
        self.assertTrue(bool(model.data(model.index(0, QueueTableModel.STEP_COLUMN), OVERRIDE_STEP_ROLE)))


if __name__ == "__main__":
    unittest.main()
