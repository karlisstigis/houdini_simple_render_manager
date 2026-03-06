from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtWidgets

from ui_core.layout_policies import (
    JOB_PROPERTIES_FALLBACK_MIN_WIDTH,
    LEFT_STACK_CONTENT_MARGIN,
    LEFT_STACK_SCROLLBAR_GAP,
    job_properties_min_width as job_properties_min_width_model,
    job_properties_target_width as job_properties_target_width_model,
    left_pane_content_floor as left_pane_content_floor_model,
    left_pane_content_required_width as left_pane_content_required_width_model,
    left_pane_required_width as left_pane_required_width_model,
)
from ui_core.theme_support import styled_scrollbar_extent

UNBOUNDED_WIDGET_MAX = int(getattr(QtWidgets, "QWIDGETSIZE_MAX", 16777215))


class WindowLayoutCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def left_stack_content_margin(self) -> int:
        return int(LEFT_STACK_CONTENT_MARGIN)

    def left_stack_scrollbar_gap(self) -> int:
        return int(LEFT_STACK_SCROLLBAR_GAP)

    def apply_startup_minimum_panel_widths(self) -> None:
        self.capture_left_pane_min_width_floor()
        self.apply_main_splitter_left_minimum_width()
        self.sync_main_splitter_left_width_lock()
        self.sync_job_properties_minimum_width_lock()
        self.apply_job_properties_minimum_width()

    @staticmethod
    def _splitter_sizes(splitter: QtWidgets.QSplitter | None) -> list[int] | None:
        if splitter is None:
            return None
        sizes = splitter.sizes()
        return sizes if len(sizes) >= 2 else None

    @staticmethod
    def _splitter_total(sizes: list[int] | None) -> int | None:
        if sizes is None:
            return None
        return int(max(sum(max(0, int(size)) for size in sizes), 1))

    def _queue_properties_splitter_state(
        self,
    ) -> tuple[QtWidgets.QSplitter | None, list[int] | None, int | None]:
        splitter = getattr(self._w, "queue_properties_splitter", None)
        sizes = self._splitter_sizes(splitter)
        return splitter, sizes, self._splitter_total(sizes)

    def schedule_panel_width_reconcile(self, delay_ms: int = 0) -> None:
        delay = max(0, int(delay_ms))
        self._w._panel_width_reconcile_timer.start(delay)

    def reconcile_panel_widths(self) -> None:
        self.sync_left_stack_scrollbar_compensation()
        self.capture_left_pane_min_width_floor()
        self.sync_main_splitter_left_width_lock()
        self.sync_job_properties_minimum_width_lock()
        self._w._apply_main_splitter_left_width_pref()
        self.apply_job_properties_minimum_width()
        self._w._update_create_job_panel_height_cap()
        self._w._maintain_left_stack_top_pack()

    def unlock_main_splitter_left_width_lock(self) -> None:
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is None:
            return
        left_widget = splitter.widget(0)
        if not isinstance(left_widget, QtWidgets.QWidget):
            return
        if int(left_widget.maximumWidth()) != UNBOUNDED_WIDGET_MAX:
            left_widget.setMaximumWidth(UNBOUNDED_WIDGET_MAX)

    def sync_main_splitter_left_width_lock(self) -> None:
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is None:
            return
        left_widget = splitter.widget(0)
        if not isinstance(left_widget, QtWidgets.QWidget):
            return
        if bool(getattr(self._w, "_main_splitter_handle_drag_active", False)):
            self.unlock_main_splitter_left_width_lock()
            return
        if bool(getattr(self._w, "_main_splitter_left_collapsed", False)):
            target_max = 0
        else:
            left_floor = int(getattr(self._w, "_left_pane_min_width_floor", 0) or 0)
            preferred_width = int(getattr(self._w, "_main_splitter_left_width_pref", 0) or 0)
            if preferred_width <= 0:
                current_width_getter = getattr(self._w, "_current_main_splitter_left_width", None)
                current_width = int(current_width_getter() or 0) if callable(current_width_getter) else 0
                target_max = UNBOUNDED_WIDGET_MAX if current_width <= 0 else max(left_floor, current_width)
            else:
                target_max = max(left_floor, preferred_width)
        if int(left_widget.maximumWidth()) != int(target_max):
            left_widget.setMaximumWidth(int(target_max))

    def unlock_job_properties_minimum_width_lock(self) -> None:
        splitter = getattr(self._w, "queue_properties_splitter", None)
        if splitter is None:
            return
        right_widget = splitter.widget(1)
        if not isinstance(right_widget, QtWidgets.QWidget):
            return
        if int(right_widget.minimumWidth()) != 0:
            right_widget.setMinimumWidth(0)

    def sync_job_properties_minimum_width_lock(self) -> None:
        splitter = getattr(self._w, "queue_properties_splitter", None)
        if splitter is None:
            return
        left_widget = splitter.widget(0)
        right_widget = splitter.widget(1)
        if isinstance(left_widget, QtWidgets.QWidget) and int(left_widget.minimumWidth()) != 0:
            left_widget.setMinimumWidth(0)
        if not isinstance(right_widget, QtWidgets.QWidget):
            return
        right_sizes = splitter.sizes()
        right_width = int(right_sizes[1]) if len(right_sizes) >= 2 else int(right_widget.width())
        if bool(getattr(self._w, "_queue_properties_handle_drag_active", False)) or right_width <= 0:
            self.unlock_job_properties_minimum_width_lock()
            return
        target_min = int(self.job_properties_min_width())
        if int(right_widget.minimumWidth()) != target_min:
            right_widget.setMinimumWidth(target_min)

    def apply_main_splitter_left_minimum_width(self) -> None:
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is None or self._w._applying_main_splitter_width:
            return
        if self._w._main_splitter_left_collapsed:
            return
        sizes = splitter.sizes()
        if len(sizes) < 2:
            return
        left_floor = int(self._w._left_pane_min_width_floor or 0)
        if left_floor <= 0:
            return
        total = int(sum(max(0, s) for s in sizes))
        if total <= 0:
            return
        right_widget = splitter.widget(1)
        right_min = int(right_widget.minimumWidth()) if right_widget is not None else 0
        target_left = min(max(left_floor, int(sizes[0])), max(left_floor, total - right_min))
        target_right = max(0, total - target_left)
        if int(sizes[0]) == int(target_left) and int(sizes[1]) == int(target_right):
            return
        self._w._applying_main_splitter_width = True
        try:
            splitter.setSizes([target_left, target_right])
        finally:
            self._w._applying_main_splitter_width = False

    def job_properties_min_width(self) -> int:
        return job_properties_min_width_model(
            (
                getattr(self._w, "job_properties_panel", None),
                getattr(self._w, "job_properties_frame", None),
            ),
            fallback=JOB_PROPERTIES_FALLBACK_MIN_WIDTH,
        )

    def job_properties_collapse_threshold(self) -> int:
        return max(1, self.job_properties_min_width() // 2)

    def job_properties_target_width(self, *, current_width: int, total_width: int) -> int:
        return job_properties_target_width_model(
            current_width=current_width,
            total_width=total_width,
            remembered_width=getattr(self._w, "_job_properties_last_width", 0),
            min_width=self.job_properties_min_width(),
        )

    def set_queue_properties_sizes(self, left_width: int, right_width: int) -> None:
        splitter, _, _ = self._queue_properties_splitter_state()
        if splitter is None:
            return
        self._w._applying_job_properties_splitter = True
        try:
            splitter.setSizes([max(0, int(left_width)), max(0, int(right_width))])
        finally:
            self._w._applying_job_properties_splitter = False

    def queue_properties_total_width(self) -> int | None:
        _, _, total = self._queue_properties_splitter_state()
        return total

    def collapse_job_properties_panel(self, *, total_width: int, remembered_width: int | None = None) -> None:
        min_width = int(self.job_properties_min_width())
        self._w._job_properties_last_width = int(max(remembered_width or 0, min_width))
        self.set_queue_properties_sizes(total_width, 0)
        self.sync_job_properties_minimum_width_lock()

    def restore_job_properties_panel(self, *, total_width: int, fallback_width: int | None = None) -> None:
        desired_right = int(
            getattr(self._w, "_job_properties_last_width", 0)
            or fallback_width
            or self.job_properties_min_width()
        )
        min_width = int(self.job_properties_min_width())
        target_right = min(max(min_width, desired_right), max(min_width, int(total_width) - 1))
        target_left = max(1, int(total_width) - target_right)
        self._w._job_properties_last_width = int(target_right)
        self.set_queue_properties_sizes(target_left, target_right)
        self.sync_job_properties_minimum_width_lock()

    def toggle_job_properties_panel(self, focus_widget: QtWidgets.QWidget | None) -> None:
        splitter, sizes, total = self._queue_properties_splitter_state()
        panel = getattr(self._w, "job_properties_panel", None)
        if splitter is None or panel is None or sizes is None or total is None:
            return
        panel_has_focus = bool(
            focus_widget is not None and (focus_widget is panel or panel.isAncestorOf(focus_widget))
        )
        if sizes[1] > 24 and panel_has_focus:
            self._w._job_properties_last_width = max(int(sizes[1]), self.job_properties_min_width())
            self.collapse_job_properties_panel(total_width=total, remembered_width=int(sizes[1]))
            queue_table = getattr(self._w, "queue_table", None)
            if queue_table is not None:
                queue_table.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)
            return
        if sizes[1] <= 24:
            self.restore_job_properties_panel(total_width=total)
        if getattr(panel, "device_mode_combo", None) is not None and panel.device_mode_combo.isEnabled():
            panel.device_mode_combo.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)
        else:
            panel.setFocus(QtCore.Qt.FocusReason.ShortcutFocusReason)

    def on_queue_properties_splitter_moved(self) -> None:
        splitter, sizes, total = self._queue_properties_splitter_state()
        if splitter is None or sizes is None or total is None or self._w._applying_job_properties_splitter:
            return
        right = int(max(0, sizes[1]))
        min_width = int(self.job_properties_min_width())
        collapse_threshold = int(self.job_properties_collapse_threshold())
        if right <= 0:
            return
        if right < collapse_threshold:
            self.collapse_job_properties_panel(total_width=total, remembered_width=right)
            return
        if right < min_width:
            self.restore_job_properties_panel(total_width=total, fallback_width=min_width)
            return
        self._w._job_properties_last_width = int(right)
        self.sync_job_properties_minimum_width_lock()

    def apply_job_properties_minimum_width(self) -> None:
        splitter, sizes, total = self._queue_properties_splitter_state()
        if splitter is None or sizes is None or total is None or self._w._applying_job_properties_splitter:
            return
        right = int(max(0, sizes[1]))
        if right <= 0:
            return
        target_right = self.job_properties_target_width(current_width=right, total_width=total)
        target_left = max(1, total - target_right)
        if int(sizes[1]) == int(target_right):
            self._w._job_properties_last_width = int(target_right)
            self.sync_job_properties_minimum_width_lock()
            return
        self._w._job_properties_last_width = int(target_right)
        self.set_queue_properties_sizes(target_left, target_right)
        self.sync_job_properties_minimum_width_lock()

    def compute_left_pane_content_required_width(self) -> int | None:
        return left_pane_content_required_width_model(
            getattr(self._w, "left_stack_host", None),
            (
                getattr(self._w, "create_job_frame", None),
                getattr(self._w, "tree_view_frame", None),
                getattr(self._w, "notifications_frame", None),
            ),
        )

    def compute_left_pane_required_width(self) -> int | None:
        return left_pane_required_width_model(
            content_required_width=self.compute_left_pane_content_required_width(),
            scrollbar_visible=self.left_stack_scrollbar_visible(),
            scrollbar_extent=styled_scrollbar_extent(getattr(self._w, "theme", {})),
            content_margin=self.left_stack_content_margin(),
            scrollbar_gap=self.left_stack_scrollbar_gap(),
        )

    def enforce_left_pane_min_width_floor(self) -> None:
        floor = self._w._left_pane_min_width_floor
        if floor is None or floor <= 0:
            return
        floor = int(floor)
        content_floor = left_pane_content_floor_model(
            pane_floor=floor,
            content_floor=self._w._left_pane_content_width_floor,
            scrollbar_visible=self.left_stack_scrollbar_visible(),
            scrollbar_extent=styled_scrollbar_extent(getattr(self._w, "theme", {})),
            content_margin=self.left_stack_content_margin(),
            scrollbar_gap=self.left_stack_scrollbar_gap(),
        )
        scroll = getattr(self._w, "left_stack_scroll", None)
        if isinstance(scroll, QtWidgets.QWidget) and int(scroll.minimumWidth()) != floor:
            scroll.setMinimumWidth(floor)
        stack_host = getattr(self._w, "left_stack_host", None)
        if isinstance(stack_host, QtWidgets.QWidget) and int(stack_host.minimumWidth()) != content_floor:
            stack_host.setMinimumWidth(content_floor)
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is not None:
            left_widget = splitter.widget(0)
            if isinstance(left_widget, QtWidgets.QWidget) and int(left_widget.minimumWidth()) != floor:
                left_widget.setMinimumWidth(floor)

    def capture_left_pane_min_width_floor(self) -> None:
        content_required = self.compute_left_pane_content_required_width()
        required = self.compute_left_pane_required_width()
        if required is None or required <= 0:
            self.enforce_left_pane_min_width_floor()
            return
        cached = self._w._left_pane_required_width_cached
        if cached is not None and int(cached) == int(required) and int(self._w._left_pane_min_width_floor or 0) == int(required):
            self.enforce_left_pane_min_width_floor()
            return
        if content_required is not None and content_required > 0:
            self._w._left_pane_content_width_floor = int(content_required)
        self._w._left_pane_required_width_cached = int(required)
        self._w._left_pane_min_width_floor = int(required)
        self.enforce_left_pane_min_width_floor()

    def left_stack_scrollbar_visible(self) -> bool:
        scroll = getattr(self._w, "left_stack_scroll", None)
        if not isinstance(scroll, QtWidgets.QScrollArea):
            return False
        bar = scroll.verticalScrollBar()
        if bar is None:
            return False
        try:
            return bool(bar.isVisible() and not bar.isHidden() and int(bar.width()) > 0)
        except Exception:
            return bool(bar.isVisible())

    def sync_left_stack_scrollbar_compensation(self) -> None:
        layout = getattr(self._w, "left_stack_layout", None)
        if not isinstance(layout, QtWidgets.QVBoxLayout):
            return
        left, top, right, bottom = layout.getContentsMargins()
        target_left = int(self.left_stack_content_margin())
        target_right = int(self.left_stack_scrollbar_gap()) if self.left_stack_scrollbar_visible() else 0
        if int(left) == int(target_left) and int(right) == int(target_right):
            return
        layout.setContentsMargins(int(target_left), int(top), int(target_right), int(bottom))

    def on_left_stack_scroll_metrics_changed(self) -> None:
        visible = self.left_stack_scrollbar_visible()
        cached_visible = getattr(self._w, "_left_stack_scrollbar_visibility_cached", None)
        if cached_visible is not None and bool(cached_visible) == bool(visible):
            return
        self._w._left_stack_scrollbar_visibility_cached = bool(visible)
        self.sync_left_stack_scrollbar_compensation()
        self.capture_left_pane_min_width_floor()
