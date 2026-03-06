from __future__ import annotations

import unittest

from PySide6 import QtCore, QtWidgets

from ui_core.window_layout_coordinator import UNBOUNDED_WIDGET_MAX, WindowLayoutCoordinator


class _DummyWindow:
    def __init__(self) -> None:
        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.left_widget = QtWidgets.QWidget()
        self.right_widget = QtWidgets.QWidget()
        self.main_splitter.addWidget(self.left_widget)
        self.main_splitter.addWidget(self.right_widget)
        self._main_splitter_handle_drag_active = False
        self._main_splitter_left_collapsed = False
        self._main_splitter_left_width_pref = 240
        self._left_pane_min_width_floor = 180
        self.queue_properties_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        self.queue_left_widget = QtWidgets.QWidget()
        self.queue_right_widget = QtWidgets.QWidget()
        self.queue_properties_splitter.addWidget(self.queue_left_widget)
        self.queue_properties_splitter.addWidget(self.queue_right_widget)
        self.queue_properties_splitter.setSizes([500, 320])
        self._queue_properties_handle_drag_active = False
        self.job_properties_panel = QtWidgets.QWidget()
        self.job_properties_frame = self.queue_right_widget

    def _current_main_splitter_left_width(self) -> int | None:
        sizes = self.main_splitter.sizes()
        if len(sizes) >= 2 and int(sizes[0]) > 0:
            return int(sizes[0])
        width = int(self.left_widget.width())
        return width if width > 0 else None


class WindowLayoutCoordinatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def test_sync_main_splitter_left_width_lock_uses_remembered_width(self) -> None:
        window = _DummyWindow()
        coordinator = WindowLayoutCoordinator(window)

        coordinator.sync_main_splitter_left_width_lock()

        self.assertEqual(window.left_widget.maximumWidth(), 240)

    def test_sync_main_splitter_left_width_lock_stays_unbounded_while_dragging(self) -> None:
        window = _DummyWindow()
        coordinator = WindowLayoutCoordinator(window)
        window._main_splitter_handle_drag_active = True

        coordinator.sync_main_splitter_left_width_lock()

        self.assertEqual(window.left_widget.maximumWidth(), UNBOUNDED_WIDGET_MAX)

    def test_sync_main_splitter_left_width_lock_uses_current_width_without_saved_preference(self) -> None:
        window = _DummyWindow()
        coordinator = WindowLayoutCoordinator(window)
        window._main_splitter_left_width_pref = None
        window.left_widget.resize(210, 100)

        coordinator.sync_main_splitter_left_width_lock()

        self.assertEqual(window.left_widget.maximumWidth(), 210)

    def test_sync_job_properties_minimum_width_lock_protects_open_panel(self) -> None:
        window = _DummyWindow()
        coordinator = WindowLayoutCoordinator(window)
        window.job_properties_panel.setMinimumWidth(280)
        window.queue_right_widget.setMinimumWidth(0)

        coordinator.sync_job_properties_minimum_width_lock()

        self.assertEqual(window.queue_left_widget.minimumWidth(), 0)
        self.assertEqual(window.queue_right_widget.minimumWidth(), 280)

    def test_sync_job_properties_minimum_width_lock_releases_while_dragging(self) -> None:
        window = _DummyWindow()
        coordinator = WindowLayoutCoordinator(window)
        window.queue_right_widget.setMinimumWidth(280)
        window._queue_properties_handle_drag_active = True

        coordinator.sync_job_properties_minimum_width_lock()

        self.assertEqual(window.queue_right_widget.minimumWidth(), 0)


if __name__ == "__main__":
    unittest.main()
