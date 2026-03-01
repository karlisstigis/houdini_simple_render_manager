from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from PySide6 import QtCore, QtGui

from queue_models import RenderJob


DISPLAY_STATUS_ROLE = QtCore.Qt.ItemDataRole.UserRole + 20
PROGRESS_BUILD_ROLE = QtCore.Qt.ItemDataRole.UserRole + 10
PROGRESS_RENDER_ROLE = QtCore.Qt.ItemDataRole.UserRole + 11
JOB_ID_ROLE = QtCore.Qt.ItemDataRole.UserRole + 30
STRICT_RANGE_ROLE = QtCore.Qt.ItemDataRole.UserRole + 31
OVERRIDE_RANGE_ROLE = QtCore.Qt.ItemDataRole.UserRole + 32
OVERRIDE_STEP_ROLE = QtCore.Qt.ItemDataRole.UserRole + 33
EDITABLE_ROLE = QtCore.Qt.ItemDataRole.UserRole + 34
PATH_SYNC_LOCKED_ROLE = QtCore.Qt.ItemDataRole.UserRole + 35


@dataclass(frozen=True)
class QueueTableModelHooks:
    jobs_provider: Callable[[], list[RenderJob]]
    is_active_job: Callable[[RenderJob], bool]
    job_phase_display: Callable[[RenderJob], str]
    job_time_remaining_display: Callable[[RenderJob], str]
    job_frame_display: Callable[[RenderJob], str]
    job_started_time_display: Callable[[RenderJob], str]
    job_end_time_display: Callable[[RenderJob], str]
    job_total_time_display: Callable[[RenderJob], str]
    queue_progress_split_values: Callable[[RenderJob], tuple[int | None, int | None]]
    edit_job_column: Callable[[int, int, str], bool]
    can_edit_job_column: Callable[[RenderJob, int], bool]
    is_job_path_sync_locked: Callable[[RenderJob], bool]
    row_style_payload: Callable[[RenderJob, int], dict[str, Any]]
    theme_icon_path: Callable[[str], str]


class QueueTableModel(QtCore.QAbstractTableModel):
    NAME_COLUMN = 0
    HIP_COLUMN = 1
    ROP_COLUMN = 2
    FRAME_RANGE_COLUMN = 3
    STEP_COLUMN = 4
    FRAME_HANDLING_COLUMN = 5
    STATUS_COLUMN = 6
    PROGRESS_COLUMN = 7
    PHASE_COLUMN = 8
    REMAINING_COLUMN = 9
    FRAME_COLUMN = 10
    FRAME_TIME_COLUMN = 11
    AVG_FRAME_TIME_COLUMN = 12
    STARTED_COLUMN = 13
    COMPLETED_COLUMN = 14
    RENDER_TIME_COLUMN = 15
    OUTPUT_COLUMN = 16

    COLUMN_HEADERS = [
        "Name",
        "HIP",
        "ROP",
        "Frame Range",
        "Step",
        "Frame Handling",
        "Status",
        "Progress",
        "Phase",
        "Remaining",
        "Frame",
        "Frame Time",
        "Avg Frame Time",
        "Started",
        "Completed",
        "Render Time",
        "Output",
    ]

    MIME_TYPE = "application/x-hsrm-queue-rows"

    def __init__(self, hooks: QueueTableModelHooks, parent: QtCore.QObject | None = None) -> None:
        super().__init__(parent)
        self._hooks = hooks

    def _jobs(self) -> list[RenderJob]:
        return self._hooks.jobs_provider()

    def _job_at(self, row: int) -> RenderJob | None:
        jobs = self._jobs()
        if 0 <= row < len(jobs):
            return jobs[row]
        return None

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._jobs())

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.COLUMN_HEADERS)

    def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role: int = QtCore.Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation == QtCore.Qt.Orientation.Horizontal and role == QtCore.Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self.COLUMN_HEADERS):
                return self.COLUMN_HEADERS[section]
        return None

    def _display_value(self, job: RenderJob, column: int) -> str:
        values = [
            job.display_name(),
            job.spec.hip_path,
            job.spec.rop_path,
            job.frame_range_display(),
            job.step_display(),
            job.frame_handling_label(),
            str(job.runtime.status.value if job.spec.enabled else "Disabled"),
            job.view.percent_text or "",
            self._hooks.job_phase_display(job),
            self._hooks.job_time_remaining_display(job),
            self._hooks.job_frame_display(job),
            job.view.prev_frame_time_text or "-",
            job.view.avg_frame_time_text or "-",
            self._hooks.job_started_time_display(job),
            self._hooks.job_end_time_display(job),
            self._hooks.job_total_time_display(job),
            job.view.out_path or "",
        ]
        return values[column]

    @staticmethod
    def _override_flags(job: RenderJob) -> tuple[bool, bool]:
        range_is_overridden = False
        step_is_overridden = False
        if job.spec.frame_range_mode == "override":
            if (
                job.runtime.runtime_start_frame is None
                or job.runtime.runtime_end_frame is None
                or job.spec.start_frame is None
                or job.spec.end_frame is None
            ):
                range_is_overridden = True
            else:
                try:
                    range_is_overridden = not (
                        int(job.spec.start_frame) == int(job.runtime.runtime_start_frame)
                        and int(job.spec.end_frame) == int(job.runtime.runtime_end_frame)
                    )
                except Exception:
                    range_is_overridden = True

            if job.runtime.runtime_step in (None, 0) or job.spec.step is None:
                step_is_overridden = True
            else:
                try:
                    step_is_overridden = int(job.spec.step) != int(float(job.runtime.runtime_step))
                except Exception:
                    step_is_overridden = True
        return range_is_overridden, step_is_overridden

    def _tooltip_for(self, job: RenderJob, column: int) -> str:
        if column == self.FRAME_HANDLING_COLUMN:
            return "How this job treats existing output frames before render."
        if column in {self.FRAME_RANGE_COLUMN, self.STEP_COLUMN}:
            range_is_overridden, step_is_overridden = self._override_flags(job)
            if job.spec.strict_frame_range:
                return "ROP frame range is Strict (node-controlled)."
            if (column == self.FRAME_RANGE_COLUMN and range_is_overridden) or (column == self.STEP_COLUMN and step_is_overridden):
                return "Overridden value."
            if job.spec.frame_range_mode == "use_rop":
                return "Using ROP value."
            if column == self.FRAME_RANGE_COLUMN:
                return "Range matches ROP value."
            return "Step matches ROP value."
        if column == self.NAME_COLUMN:
            return job.runtime.log_file_path or ""
        if column == self.OUTPUT_COLUMN:
            return job.view.out_path or ""
        return ""

    def _decoration_for(self, job: RenderJob, column: int) -> QtGui.QIcon | None:
        if column not in {self.FRAME_RANGE_COLUMN, self.STEP_COLUMN}:
            return None
        if job.spec.strict_frame_range:
            path = self._hooks.theme_icon_path("lock_orange")
            return QtGui.QIcon(path) if path else QtGui.QIcon()
        range_is_overridden, step_is_overridden = self._override_flags(job)
        if (column == self.FRAME_RANGE_COLUMN and range_is_overridden) or (column == self.STEP_COLUMN and step_is_overridden):
            path = self._hooks.theme_icon_path("override_dot_red")
            return QtGui.QIcon(path) if path else QtGui.QIcon()
        return QtGui.QIcon()

    def data(self, index: QtCore.QModelIndex, role: int = QtCore.Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None
        job = self._job_at(index.row())
        if job is None:
            return None
        column = index.column()

        if role == QtCore.Qt.ItemDataRole.DisplayRole:
            return self._display_value(job, column)
        if role == QtCore.Qt.ItemDataRole.EditRole:
            if column == self.NAME_COLUMN:
                return job.spec.name
            if column == self.HIP_COLUMN:
                return job.spec.hip_path
            if column == self.ROP_COLUMN:
                return job.spec.rop_path
            if column == self.FRAME_RANGE_COLUMN:
                return job.frame_range_display()
            if column == self.STEP_COLUMN:
                return job.step_display()
            if column == self.FRAME_HANDLING_COLUMN:
                return job.frame_handling_label()
            return self._display_value(job, column)
        if role == QtCore.Qt.ItemDataRole.ToolTipRole:
            return self._tooltip_for(job, column)
        if role == QtCore.Qt.ItemDataRole.DecorationRole:
            return self._decoration_for(job, column)
        if role == QtCore.Qt.ItemDataRole.BackgroundRole:
            payload = self._hooks.row_style_payload(job, index.row())
            return payload.get("background")
        if role == QtCore.Qt.ItemDataRole.ForegroundRole:
            payload = self._hooks.row_style_payload(job, index.row())
            return payload.get("foreground")
        if role == DISPLAY_STATUS_ROLE:
            return str(job.runtime.status.value)
        if role == PROGRESS_BUILD_ROLE and column == self.PROGRESS_COLUMN:
            return self._hooks.queue_progress_split_values(job)[0]
        if role == PROGRESS_RENDER_ROLE and column == self.PROGRESS_COLUMN:
            return self._hooks.queue_progress_split_values(job)[1]
        if role == JOB_ID_ROLE:
            return job.id
        if role == STRICT_RANGE_ROLE:
            return bool(job.spec.strict_frame_range)
        if role == OVERRIDE_RANGE_ROLE:
            return self._override_flags(job)[0]
        if role == OVERRIDE_STEP_ROLE:
            return self._override_flags(job)[1]
        if role == EDITABLE_ROLE:
            return self._hooks.can_edit_job_column(job, column)
        if role == PATH_SYNC_LOCKED_ROLE:
            return bool(self._hooks.is_job_path_sync_locked(job))
        return None

    def flags(self, index: QtCore.QModelIndex) -> QtCore.Qt.ItemFlags:
        if not index.isValid():
            return QtCore.Qt.ItemFlag.NoItemFlags
        job = self._job_at(index.row())
        if job is None:
            return QtCore.Qt.ItemFlag.NoItemFlags
        flags = (
            QtCore.Qt.ItemFlag.ItemIsEnabled
            | QtCore.Qt.ItemFlag.ItemIsSelectable
            | QtCore.Qt.ItemFlag.ItemIsDragEnabled
        )
        if self._hooks.can_edit_job_column(job, index.column()):
            flags |= QtCore.Qt.ItemFlag.ItemIsEditable
        return flags

    def setData(self, index: QtCore.QModelIndex, value: Any, role: int = QtCore.Qt.ItemDataRole.EditRole) -> bool:
        if role not in {QtCore.Qt.ItemDataRole.EditRole, QtCore.Qt.ItemDataRole.DisplayRole}:
            return False
        if not index.isValid():
            return False
        return bool(self._hooks.edit_job_column(index.row(), index.column(), str(value or "")))

    def mimeTypes(self) -> list[str]:
        return [self.MIME_TYPE]

    def mimeData(self, indexes: list[QtCore.QModelIndex]) -> QtCore.QMimeData | None:
        if not indexes:
            return None
        rows = sorted({index.row() for index in indexes if index.isValid()})
        mime = QtCore.QMimeData()
        mime.setData(self.MIME_TYPE, ",".join(str(row) for row in rows).encode("utf-8"))
        return mime

    def refresh_all(self) -> None:
        self.beginResetModel()
        self.endResetModel()

    def refresh_job_by_id(self, job_id: str) -> None:
        self.refresh_jobs_by_id([job_id])

    def refresh_jobs_by_id(self, job_ids: list[str]) -> None:
        wanted = {str(job_id or "").strip() for job_id in job_ids if str(job_id or "").strip()}
        if not wanted:
            return
        for row, job in enumerate(self._jobs()):
            if job.id not in wanted:
                continue
            top_left = self.index(row, 0)
            bottom_right = self.index(row, self.columnCount() - 1)
            self.dataChanged.emit(top_left, bottom_right)
