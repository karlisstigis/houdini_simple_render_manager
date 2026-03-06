from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtWidgets

from ui_core.layout_policies import splitter_total_size as splitter_total_size_model
from ui_core.widgets import PanelFrame


UNBOUNDED_WIDGET_MAX = int(getattr(QtWidgets, "QWIDGETSIZE_MAX", 16777215))


class PanelSplitterCoordinator:
    def __init__(self, window: Any) -> None:
        self._w = window

    def apply_left_splitter_default_sizes(self) -> None:
        splitter = getattr(self._w, "left_vertical_splitter", None)
        create_panel = getattr(self._w, "create_job_frame", None)
        if splitter is None or create_panel is None:
            return
        total_height = splitter.height()
        if total_height <= 0:
            total_height = splitter.sizeHint().height()
        if total_height <= 0:
            return
        preferred_top = getattr(self._w, "_left_splitter_top_height_pref", None)
        top_target = preferred_top if preferred_top is not None else max(
            create_panel.minimumSizeHint().height(),
            create_panel.sizeHint().height(),
        )
        top_target = max(1, min(top_target, max(1, total_height - 80)))
        bottom_target = max(1, total_height - top_target)
        splitter.setSizes([top_target, bottom_target])

    def apply_left_column_splitter_default_sizes(self) -> None:
        splitter = getattr(self._w, "left_column_splitter", None)
        notifications_panel = getattr(self._w, "notifications_frame", None)
        if splitter is None or notifications_panel is None:
            return
        total_height = splitter.height()
        if total_height <= 0:
            total_height = splitter.sizeHint().height()
        if total_height <= 0:
            return
        preferred_bottom = getattr(self._w, "_left_notifications_height_pref", None)
        bottom_target = preferred_bottom if preferred_bottom is not None else max(
            notifications_panel.minimumSizeHint().height(),
            min(170, notifications_panel.sizeHint().height()),
        )
        bottom_target = max(80, min(bottom_target, max(80, total_height - 160)))
        top_target = max(120, total_height - bottom_target)
        splitter.setSizes([top_target, bottom_target])

    def apply_left_stack_splitter_default_sizes(self) -> None:
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is None or self._w._left_stack_sizes_initialized:
            return
        self.update_create_job_panel_height_cap()
        sizes_pref = self._w._left_stack_splitter_sizes_pref
        default_pref = [
            int(max(80, getattr(self._w, "create_job_frame", splitter).sizeHint().height())),
            int(max(80, getattr(self._w, "tree_view_frame", splitter).sizeHint().height())),
            int(max(70, getattr(self._w, "notifications_frame", splitter).sizeHint().height())),
        ]
        if isinstance(sizes_pref, list) and len(sizes_pref) >= 3:
            panel_pref = [int(max(1, sizes_pref[0])), int(max(1, sizes_pref[1])), int(max(1, sizes_pref[2]))]
            if not self._w._left_stack_user_resized_this_session:
                inflated = any(panel_pref[i] > int(default_pref[i] * 2.25) for i in range(3))
                if inflated:
                    panel_pref = list(default_pref)
        else:
            panel_pref = list(default_pref)
        self.apply_left_stack_panel_heights(panel_pref)
        self._w._left_stack_sizes_initialized = True

    def update_create_job_panel_height_cap(self) -> None:
        panel = getattr(self._w, "create_job_frame", None)
        if not isinstance(panel, PanelFrame) or not panel.is_expanded():
            return
        content_hint = int(max(1, panel.sizeHint().height()))
        collapsed_h = int(panel.collapsed_height_hint())
        panel.set_expanded_max_height(max(content_hint + 6, collapsed_h + 6))

    def update_left_panel_expanded_min_heights(self) -> None:
        create_panel = getattr(self._w, "create_job_frame", None)
        if isinstance(create_panel, PanelFrame):
            create_hint = int(max(create_panel.minimumSizeHint().height(), create_panel.sizeHint().height()))
            create_panel.set_expanded_min_height(max(create_hint, create_panel.collapsed_height_hint() + 40))
        tree_panel = getattr(self._w, "tree_view_frame", None)
        if isinstance(tree_panel, PanelFrame):
            tree_hint = int(max(tree_panel.minimumSizeHint().height(), tree_panel.sizeHint().height()))
            tree_panel.set_expanded_min_height(max(tree_hint, tree_panel.collapsed_height_hint() + 40))

    def apply_left_stack_panel_heights(self, panel_pref: list[int] | tuple[int, int, int]) -> None:
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is None or len(panel_pref) < 3:
            return
        self.update_create_job_panel_height_cap()
        total_height = splitter.height()
        if total_height <= 0:
            total_height = splitter.sizeHint().height()
        if total_height <= 0:
            return
        targets = [max(1, int(panel_pref[0])), max(1, int(panel_pref[1])), max(1, int(panel_pref[2]))]
        create_panel = splitter.widget(0)
        if isinstance(create_panel, PanelFrame):
            create_cap = int(create_panel.maximumHeight())
            if 0 < create_cap < UNBOUNDED_WIDGET_MAX:
                targets[0] = min(targets[0], create_cap)
        min_targets: list[int] = []
        for idx in range(3):
            widget = splitter.widget(idx)
            if isinstance(widget, PanelFrame):
                min_h = int(max(1, widget.collapsed_height_hint()))
            elif widget is not None:
                min_h = int(max(1, widget.minimumHeight()))
            else:
                min_h = 1
            min_targets.append(max(1, min_h))
            targets[idx] = max(min_targets[idx], targets[idx])

        max_panel_total = max(3, total_height - 1)
        panel_total = int(sum(targets))
        if panel_total > max_panel_total:
            scale = float(max_panel_total) / float(panel_total)
            scaled = [max(1, int(round(v * scale))) for v in targets]
            for idx in range(3):
                scaled[idx] = max(1, min_targets[idx], scaled[idx])
            overflow = int(sum(scaled)) - max_panel_total
            if overflow > 0:
                for idx in reversed(range(3)):
                    if overflow <= 0:
                        break
                    reducible = max(0, scaled[idx] - min_targets[idx])
                    if reducible <= 0:
                        continue
                    cut = min(reducible, overflow)
                    scaled[idx] -= cut
                    overflow -= cut
            targets = scaled

        spacer = int(total_height - sum(targets))
        if spacer < 1:
            deficit = 1 - spacer
            for idx in reversed(range(3)):
                reducible = max(0, targets[idx] - min_targets[idx])
                if reducible <= 0:
                    continue
                cut = min(reducible, deficit)
                targets[idx] -= cut
                deficit -= cut
                if deficit <= 0:
                    break
            spacer = max(1, int(total_height - sum(targets)))

        new_sizes = [int(targets[0]), int(targets[1]), int(targets[2]), int(spacer)]
        current_sizes = splitter.sizes()
        if len(current_sizes) == 4 and all(int(current_sizes[i]) == int(new_sizes[i]) for i in range(4)):
            self._w._left_stack_splitter_sizes_pref = [int(max(0, s)) for s in current_sizes]
            return
        self._w._applying_panel_collapse_layout = True
        try:
            splitter.setSizes(new_sizes)
        finally:
            self._w._applying_panel_collapse_layout = False
        self._w._left_stack_splitter_sizes_pref = [int(max(0, s)) for s in splitter.sizes()]

    def register_collapsible_panel(
        self,
        panel: QtWidgets.QWidget | None,
        splitter: QtWidgets.QSplitter | None,
        index: int,
    ) -> None:
        if panel is None or splitter is None or not isinstance(panel, PanelFrame):
            return
        panel.expanded_changed.connect(
            lambda expanded, p=panel, s=splitter, i=int(index): self.on_panel_expanded_changed(p, s, i, expanded)
        )

    def on_panel_expanded_changed(
        self,
        panel: PanelFrame,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
    ) -> None:
        main_left_before = self.current_main_splitter_left_width()
        if main_left_before is None:
            pref = self._w._main_splitter_left_width_pref
            if pref is not None and int(pref) > 0:
                main_left_before = int(pref)
        self.rebalance_splitter_for_panel_toggle(
            panel=panel,
            splitter=splitter,
            index=int(index),
            expanded=bool(expanded),
        )
        if splitter in {getattr(self._w, "left_vertical_splitter", None), getattr(self._w, "left_column_splitter", None)}:
            QtCore.QTimer.singleShot(0, self.pack_left_column_top)
        if main_left_before is not None and main_left_before > 0:
            self._w._main_splitter_left_width_pref = int(main_left_before)
            self.restore_main_splitter_left_width_deferred(int(main_left_before))

    def on_left_stack_panel_expanded_changed(self, panel: PanelFrame, index: int, expanded: bool) -> None:
        main_left_before = self.current_main_splitter_left_width()
        if main_left_before is None:
            pref = self._w._main_splitter_left_width_pref
            if pref is not None and int(pref) > 0:
                main_left_before = int(pref)
        if main_left_before is None:
            left_widget = getattr(self._w, "left_stack_scroll", None)
            if isinstance(left_widget, QtWidgets.QWidget):
                width = int(left_widget.width())
                if width > 0:
                    main_left_before = width
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is not None:
            sizes = splitter.sizes()
            if len(sizes) >= 4:
                if (not bool(expanded)) and int(index) in {0, 1, 2} and int(sizes[index]) > 0:
                    self._w._panel_restore_sizes[(id(splitter), int(index))] = int(sizes[index])
                panel_heights = [int(sizes[0]), int(sizes[1]), int(sizes[2])]
                for idx in range(3):
                    widget = splitter.widget(idx)
                    if isinstance(widget, PanelFrame) and (not widget.is_expanded()):
                        panel_heights[idx] = int(max(1, widget.collapsed_height_hint()))
                if bool(expanded) and int(index) in {0, 1, 2}:
                    key = (id(splitter), int(index))
                    restore = int(self._w._panel_restore_sizes.get(key, panel_heights[index]))
                    collapsed_h = int(max(1, panel.collapsed_height_hint()))
                    if restore <= (collapsed_h + 6):
                        pref = self._w._left_stack_splitter_sizes_pref
                        pref_restore = None
                        if isinstance(pref, list) and len(pref) >= 3:
                            try:
                                pref_restore = int(pref[int(index)])
                            except Exception:
                                pref_restore = None
                        hint_restore = int(max(collapsed_h + 40, panel.sizeHint().height()))
                        restore = int(pref_restore) if pref_restore is not None and pref_restore > (collapsed_h + 6) else int(hint_restore)
                    self._w._panel_restore_sizes[key] = int(restore)
                    panel_heights[index] = max(panel_heights[index], restore)
                self.apply_left_stack_panel_heights(panel_heights)
            self._w._left_stack_sizes_initialized = True
        self._w._enforce_left_pane_min_width_floor()
        if main_left_before is not None and main_left_before > 0:
            self._w._main_splitter_left_width_pref = int(main_left_before)
            self.restore_main_splitter_left_width_deferred(int(main_left_before))

    def current_main_splitter_left_width(self) -> int | None:
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is None:
            return None
        sizes = splitter.sizes()
        if len(sizes) >= 2 and int(sizes[0]) > 0:
            return int(sizes[0])
        left_widget = splitter.widget(0)
        if left_widget is not None:
            widget_width = int(left_widget.width())
            if widget_width > 0:
                return widget_width
        return None

    def restore_main_splitter_left_width_deferred(self, left_width: int) -> None:
        target = int(max(1, left_width))
        QtCore.QTimer.singleShot(0, lambda left=target: self.restore_main_splitter_left_width(left))
        QtCore.QTimer.singleShot(80, lambda left=target: self.restore_main_splitter_left_width(left))

    def restore_main_splitter_left_width(self, left_width: int) -> None:
        splitter = getattr(self._w, "main_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) < 2:
            return
        total = int(splitter_total_size_model(sizes))
        if total <= 0:
            return
        left_widget = splitter.widget(0)
        right_widget = splitter.widget(1)
        left_min = int(left_widget.minimumWidth()) if left_widget is not None else 0
        right_min = int(right_widget.minimumWidth()) if right_widget is not None else 0
        target_left = max(left_min, min(int(left_width), max(left_min, total - right_min)))
        target_right = max(0, total - target_left)
        if int(sizes[0]) == int(target_left) and int(sizes[1]) == int(target_right):
            return
        self._w._applying_main_splitter_width = True
        try:
            splitter.setSizes([target_left, target_right])
        finally:
            self._w._applying_main_splitter_width = False

    def pack_left_column_top(self) -> None:
        splitter = getattr(self._w, "left_column_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) < 2:
            return
        total = int(splitter_total_size_model(sizes))
        if total <= 0:
            return
        top_widget = splitter.widget(0)
        bottom_widget = splitter.widget(1)
        if top_widget is None or bottom_widget is None:
            return
        top_target = int(max(1, top_widget.minimumSizeHint().height()))
        bottom_min = int(max(1, bottom_widget.minimumSizeHint().height()))
        top_target = max(1, min(top_target, max(1, total - bottom_min)))
        self.apply_splitter_sizes(splitter, [top_target, max(1, total - top_target)])

    def apply_splitter_sizes(self, splitter: QtWidgets.QSplitter, sizes: list[int] | tuple[int, ...]) -> None:
        target_sizes = [int(max(1, s)) for s in sizes]
        current_sizes = splitter.sizes()
        if len(current_sizes) == len(target_sizes) and all(int(current_sizes[i]) == int(target_sizes[i]) for i in range(len(target_sizes))):
            return
        self._w._applying_panel_collapse_layout = True
        try:
            splitter.setSizes(target_sizes)
        finally:
            self._w._applying_panel_collapse_layout = False

    def _handle_right_queue_logs_special_toggle(
        self,
        *,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
        total: int,
        top_widget: PanelFrame,
        bottom_widget: PanelFrame,
    ) -> bool:
        if splitter is not getattr(self._w, "right_vertical_splitter", None) or bool(expanded):
            return False
        if int(index) == 0:
            if not bottom_widget.is_expanded():
                bottom_widget.set_expanded(True)
            top_size = int(max(1, min(top_widget.collapsed_height_hint(), total - 1 if total > 1 else 1)))
            self.apply_splitter_sizes(splitter, [top_size, max(1, total - top_size)])
            return True
        if int(index) == 1 and (not top_widget.is_expanded()):
            if not top_widget.is_expanded():
                top_widget.set_expanded(True)
            bottom_size = int(max(1, min(bottom_widget.collapsed_height_hint(), total - 1 if total > 1 else 1)))
            self.apply_splitter_sizes(splitter, [max(1, total - bottom_size), bottom_size])
            return True
        return False

    def rebalance_splitter_for_panel_toggle(
        self,
        *,
        panel: PanelFrame,
        splitter: QtWidgets.QSplitter,
        index: int,
        expanded: bool,
    ) -> None:
        sizes = splitter.sizes()
        if len(sizes) < 2 or index < 0 or index >= len(sizes):
            return
        total = int(splitter_total_size_model(sizes))
        if total <= 0:
            return
        key = (id(splitter), int(index))
        this_size = int(max(0, sizes[index]))
        other_index = 1 - index if len(sizes) == 2 else -1
        collapsed_h = int(max(1, panel.collapsed_height_hint()))
        if not expanded and this_size > (collapsed_h + 6):
            self._w._panel_restore_sizes[key] = this_size

        if len(sizes) == 2:
            top_widget = splitter.widget(0)
            bottom_widget = splitter.widget(1)
            if isinstance(top_widget, PanelFrame) and isinstance(bottom_widget, PanelFrame):
                if self._handle_right_queue_logs_special_toggle(
                    splitter=splitter,
                    index=int(index),
                    expanded=bool(expanded),
                    total=total,
                    top_widget=top_widget,
                    bottom_widget=bottom_widget,
                ):
                    return
                top_collapsed = not top_widget.is_expanded()
                bottom_collapsed = not bottom_widget.is_expanded()
                top_collapsed_h = int(max(1, top_widget.collapsed_height_hint()))
                bottom_collapsed_h = int(max(1, bottom_widget.collapsed_height_hint()))
                top_min = int(max(1, top_widget.minimumSizeHint().height()))
                bottom_min = int(max(1, bottom_widget.minimumSizeHint().height()))
                if top_collapsed:
                    top_size = max(1, min(top_collapsed_h, total - 1 if total > 1 else 1))
                    new_sizes = [top_size, max(1, total - top_size)]
                elif bottom_collapsed:
                    bottom_size = max(1, min(bottom_collapsed_h, total - 1 if total > 1 else 1))
                    new_sizes = [max(1, total - bottom_size), bottom_size]
                else:
                    top_restore = int(self._w._panel_restore_sizes.get((id(splitter), 0), sizes[0]))
                    bottom_restore = int(self._w._panel_restore_sizes.get((id(splitter), 1), sizes[1]))
                    top_restore = max(top_restore, top_collapsed_h + 12)
                    bottom_restore = max(bottom_restore, bottom_collapsed_h + 12)
                    if int(index) == 0 and bool(expanded):
                        top_size = max(top_min, min(top_restore, max(top_min, total - bottom_min)))
                    elif int(index) == 1 and bool(expanded):
                        bottom_size = max(bottom_min, min(bottom_restore, max(bottom_min, total - top_min)))
                        top_size = max(top_min, total - bottom_size)
                    else:
                        top_size = max(top_min, min(int(sizes[0]), max(top_min, total - bottom_min)))
                    new_sizes = [top_size, max(1, total - top_size)]
                self.apply_splitter_sizes(splitter, new_sizes)
                return

        if expanded:
            target = int(self._w._panel_restore_sizes.get(key, this_size))
            target = max(target, int(panel.minimumSizeHint().height()), collapsed_h + 12)
            if len(sizes) == 2 and other_index >= 0:
                other_widget = splitter.widget(other_index)
                other_min = int(other_widget.minimumSizeHint().height()) if other_widget is not None else 0
                target = min(target, max(int(panel.minimumSizeHint().height()), total - other_min))
        else:
            target = int(collapsed_h)
            if len(sizes) == 2 and other_index >= 0:
                other_widget = splitter.widget(other_index)
                other_min = int(other_widget.minimumSizeHint().height()) if other_widget is not None else 0
                target = min(target, max(int(panel.minimumSizeHint().height()), total - other_min))
        target = max(1, min(target, total - 1 if total > 1 else 1))
        new_sizes = list(sizes)
        new_sizes[index] = target
        if len(sizes) == 2 and other_index >= 0:
            new_sizes[other_index] = max(1, total - target)
        self.apply_splitter_sizes(splitter, new_sizes)

    def on_left_splitter_moved(self) -> None:
        if self._w._applying_panel_collapse_layout:
            return
        splitter = getattr(self._w, "left_vertical_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) >= 2:
            self._w._left_splitter_top_height_pref = int(max(0, sizes[0]))

    def on_left_column_splitter_moved(self) -> None:
        if self._w._applying_panel_collapse_layout:
            return
        splitter = getattr(self._w, "left_column_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) >= 2:
            self._w._left_notifications_height_pref = int(max(0, sizes[1]))

    def on_left_stack_splitter_moved(self) -> None:
        if self._w._applying_panel_collapse_layout:
            return
        if not (QtWidgets.QApplication.mouseButtons() & QtCore.Qt.MouseButton.LeftButton):
            return
        self._w._left_stack_user_resized_this_session = True
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if self.auto_collapse_left_stack_panels_from_sizes(sizes):
            sizes = splitter.sizes()
        if len(sizes) == 4:
            saved = [int(max(0, s)) for s in sizes]
            for idx in range(3):
                widget = splitter.widget(idx)
                if isinstance(widget, PanelFrame) and (not widget.is_expanded()):
                    saved[idx] = int(max(saved[idx], widget.collapsed_height_hint()))
            self._w._left_stack_splitter_sizes_pref = saved
            self._w._left_stack_sizes_initialized = True

    def on_right_splitter_moved(self) -> None:
        if self._w._applying_panel_collapse_layout:
            return
        splitter = getattr(self._w, "right_vertical_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        if len(sizes) < 2:
            return
        for idx in range(2):
            if self.maybe_auto_collapse_splitter_panel(splitter=splitter, sizes=sizes, index=idx):
                break

    def maybe_auto_collapse_splitter_panel(
        self,
        *,
        splitter: QtWidgets.QSplitter,
        sizes: list[int] | tuple[int, ...],
        index: int,
    ) -> bool:
        if int(index) < 0 or int(index) >= len(sizes):
            return False
        widget = splitter.widget(int(index))
        if not isinstance(widget, PanelFrame) or not widget.is_expanded():
            return False
        current_height = int(max(0, sizes[int(index)]))
        collapsed_height = int(max(1, widget.collapsed_height_hint()))
        collapse_trigger = int(max(1, collapsed_height * 2))
        key = (id(splitter), int(index))
        if current_height >= collapse_trigger:
            self._w._panel_restore_sizes[key] = int(current_height)
            return False
        restore_height = int(self._w._panel_restore_sizes.get(key, 0))
        restore_floor = max(int(widget.minimumSizeHint().height()), collapse_trigger + 12)
        self._w._panel_restore_sizes[key] = max(restore_height, int(current_height), restore_floor)
        widget.set_expanded(False)
        return True

    def maintain_left_stack_top_pack(self) -> None:
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is None:
            return
        sizes_pref = self._w._left_stack_splitter_sizes_pref
        if not (isinstance(sizes_pref, list) and len(sizes_pref) >= 3):
            return
        panel_pref = [int(max(1, sizes_pref[0])), int(max(1, sizes_pref[1])), int(max(1, sizes_pref[2]))]
        self.apply_left_stack_panel_heights(panel_pref)

    def auto_collapse_left_stack_panels_from_sizes(self, sizes: list[int] | tuple[int, ...]) -> bool:
        splitter = getattr(self._w, "left_stack_splitter", None)
        if splitter is None or len(sizes) < 3:
            return False
        collapsed_any = False
        for idx in range(3):
            widget = splitter.widget(idx)
            if not isinstance(widget, PanelFrame) or (not widget.is_expanded()):
                continue
            current_height = int(max(0, sizes[idx]))
            collapsed_height = int(max(1, widget.collapsed_height_hint()))
            restore_height = int(self._w._panel_restore_sizes.get((id(splitter), idx), max(collapsed_height + 1, current_height)))
            if current_height <= (collapsed_height + 6):
                self._w._panel_restore_sizes[(id(splitter), idx)] = max(restore_height, current_height, collapsed_height + 1)
                widget.set_expanded(False)
                collapsed_any = True
        return collapsed_any
