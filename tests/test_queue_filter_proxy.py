from __future__ import annotations

import unittest

from PySide6 import QtGui, QtWidgets

from queue_core.queue_filter_proxy import QueueFilterProxyModel
from queue_core.queue_models import JobStatus
from queue_core.queue_table_model import DISPLAY_STATUS_ROLE


class QueueFilterProxyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def test_offline_filter_includes_disabled_offline_jobs(self) -> None:
        model = QtGui.QStandardItemModel(0, 18)
        disabled_offline_item = QtGui.QStandardItem("Disabled")
        disabled_offline_item.setData(JobStatus.OFFLINE.value, DISPLAY_STATUS_ROLE)
        model.setItem(0, 6, disabled_offline_item)
        model.setItem(0, 0, QtGui.QStandardItem("job-a"))
        model.setItem(1, 6, QtGui.QStandardItem("Queued"))
        model.setItem(1, 0, QtGui.QStandardItem("job-b"))

        proxy = QueueFilterProxyModel()
        proxy.setSourceModel(model)
        proxy.set_status_filter(JobStatus.OFFLINE.value)

        self.assertEqual(proxy.rowCount(), 1)
        self.assertEqual(proxy.index(0, 0).data(), "job-a")

    def test_disabled_filter_uses_display_status(self) -> None:
        model = QtGui.QStandardItemModel(0, 18)
        disabled_item = QtGui.QStandardItem("Disabled")
        disabled_item.setData(JobStatus.QUEUED.value, DISPLAY_STATUS_ROLE)
        model.setItem(0, 6, disabled_item)
        model.setItem(0, 0, QtGui.QStandardItem("job-a"))
        offline_item = QtGui.QStandardItem("Offline")
        offline_item.setData(JobStatus.OFFLINE.value, DISPLAY_STATUS_ROLE)
        model.setItem(1, 6, offline_item)
        model.setItem(1, 0, QtGui.QStandardItem("job-b"))

        proxy = QueueFilterProxyModel()
        proxy.setSourceModel(model)
        proxy.set_status_filter("Disabled")

        self.assertEqual(proxy.rowCount(), 1)
        self.assertEqual(proxy.index(0, 0).data(), "job-a")


if __name__ == "__main__":
    unittest.main()
