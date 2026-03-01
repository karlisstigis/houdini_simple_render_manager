from __future__ import annotations

from PySide6 import QtCore

from queue_models import JobStatus


class QueueFilterProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent: QtCore.QObject | None = None) -> None:
        super().__init__(parent)
        self._search_text = ""
        self._status_filter = ""
        self._enabled_only = False
        self.setDynamicSortFilter(True)
        self.setFilterCaseSensitivity(QtCore.Qt.CaseSensitivity.CaseInsensitive)

    def set_search_text(self, text: str) -> None:
        normalized = str(text or "").strip().lower()
        if normalized == self._search_text:
            return
        self._search_text = normalized
        self.invalidateFilter()

    def set_status_filter(self, status_value: str) -> None:
        normalized = str(status_value or "").strip()
        if normalized == self._status_filter:
            return
        self._status_filter = normalized
        self.invalidateFilter()

    def set_enabled_only(self, enabled_only: bool) -> None:
        value = bool(enabled_only)
        if value == self._enabled_only:
            return
        self._enabled_only = value
        self.invalidateFilter()

    def has_active_filters(self) -> bool:
        return bool(self._search_text or self._status_filter or self._enabled_only)

    def filterAcceptsRow(self, source_row: int, source_parent: QtCore.QModelIndex) -> bool:
        model = self.sourceModel()
        if model is None:
            return True

        if self._enabled_only:
            status_index = model.index(source_row, 6, source_parent)
            status_value = str(model.data(status_index, QtCore.Qt.ItemDataRole.DisplayRole) or "")
            if status_value == "Disabled":
                return False

        if self._status_filter:
            status_index = model.index(source_row, 6, source_parent)
            status_value = str(model.data(status_index, QtCore.Qt.ItemDataRole.DisplayRole) or "")
            if status_value != self._status_filter:
                return False

        if not self._search_text:
            return True

        for column in (0, 1, 2, 16):
            idx = model.index(source_row, column, source_parent)
            text = str(model.data(idx, QtCore.Qt.ItemDataRole.DisplayRole) or "").lower()
            if self._search_text in text:
                return True
        return False


QUEUE_STATUS_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("All", ""),
    ("Queued", JobStatus.QUEUED.value),
    ("Running", JobStatus.RUNNING.value),
    ("Done", JobStatus.DONE.value),
    ("Failed", JobStatus.FAILED.value),
    ("Canceled", JobStatus.CANCELED.value),
    ("Interrupted", JobStatus.INTERRUPTED.value),
    ("Offline", JobStatus.OFFLINE.value),
    ("Disabled", "Disabled"),
]
