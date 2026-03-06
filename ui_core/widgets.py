from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Callable

from PySide6 import QtCore, QtGui, QtWidgets

from queue_core.queue_models import DeviceOverrideMode, RenderJob, UsdOutputDirectoryMode
from queue_core.queue_table_model import PATH_SYNC_LOCKED_ROLE, QueueTableModel
from ui_core.theme_support import DEFAULT_THEME, styled_scrollbar_extent

TABLER_NOTIFICATION_NOTICE_HTML = (
    "<b>Notification Icons</b><br>"
    "This app bundles notification icons from "
    '<a href="https://new.tabler.io/icons"><span style="color:#ffffff; text-decoration:underline;">Tabler Icons</span></a> by Pawel Kuna. '
    "They are provided under the MIT License.<br><br>"
    'Source pages: <a href="https://tabler.io/icons/icon/alert-triangle"><span style="color:#ffffff; text-decoration:underline;">alert-triangle</span></a>, '
    '<a href="https://tabler.io/icons/icon/alert-octagon"><span style="color:#ffffff; text-decoration:underline;">alert-octagon</span></a>, '
    '<a href="https://tabler.io/icons/icon/info-circle"><span style="color:#ffffff; text-decoration:underline;">info-circle</span></a>.<br>'
    "Bundled license file: <code>assets/third_party/tabler/LICENSE.txt</code>"
)

CREATOR_NOTICE_HTML = (
    "<b>Houdini Simple Render Manager</b><br>"
    "Karlis Stigis<br>"
    '<a href="https://karlisstigis.com"><span style="color:#ffffff; text-decoration:underline;">https://karlisstigis.com</span></a>'
)


class CleanStepSpinBox(QtWidgets.QSpinBox):
    """Spin box that does not auto-select text when stepped via arrow buttons."""

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._clicked_button = False

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        self._clicked_button = False
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            opt = QtWidgets.QStyleOptionSpinBox()
            self.initStyleOption(opt)
            up_rect = self.style().subControlRect(
                QtWidgets.QStyle.ComplexControl.CC_SpinBox,
                opt,
                QtWidgets.QStyle.SubControl.SC_SpinBoxUp,
                self,
            )
            down_rect = self.style().subControlRect(
                QtWidgets.QStyle.ComplexControl.CC_SpinBox,
                opt,
                QtWidgets.QStyle.SubControl.SC_SpinBoxDown,
                self,
            )
            if up_rect.contains(event.position().toPoint()) or down_rect.contains(event.position().toPoint()):
                self._clicked_button = True
        super().mousePressEvent(event)
        if self._clicked_button:
            QtCore.QTimer.singleShot(0, self._clear_line_edit_selection)
        elif event.button() == QtCore.Qt.MouseButton.LeftButton:
            QtCore.QTimer.singleShot(0, self._select_line_edit_text)

    def stepBy(self, steps: int) -> None:
        super().stepBy(steps)
        if self._clicked_button:
            QtCore.QTimer.singleShot(0, self._clear_line_edit_selection)

    def _clear_line_edit_selection(self) -> None:
        line = self.lineEdit()
        if line is not None:
            line.deselect()
            line.clearFocus()
        self.clearFocus()

    def _select_line_edit_text(self) -> None:
        line = self.lineEdit()
        if line is not None:
            line.setFocus(QtCore.Qt.FocusReason.MouseFocusReason)
            line.selectAll()


class SafeCommitLineEdit(QtWidgets.QLineEdit):
    commit_requested = QtCore.Signal(str)
    cancel_requested = QtCore.Signal()

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._commit_emitted = False
        self._commit_via_enter = False

    def focusOutEvent(self, event: QtGui.QFocusEvent) -> None:
        super().focusOutEvent(event)
        self._emit_commit_once()

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        if event.key() in {QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter}:
            self._commit_via_enter = True
            self._emit_commit_once()
            event.accept()
            return
        if event.key() == QtCore.Qt.Key.Key_Escape:
            if not self._commit_emitted:
                self._commit_emitted = True
                self.cancel_requested.emit()
            event.accept()
            return
        super().keyPressEvent(event)

    def _emit_commit_once(self) -> None:
        if self._commit_emitted:
            return
        self._commit_emitted = True
        self.commit_requested.emit(self.text())

    def committed_via_enter(self) -> bool:
        return self._commit_via_enter


class PanelFrame(QtWidgets.QFrame):
    """Header-strip panel wrapper used for top-level app sections."""
    expanded_changed = QtCore.Signal(bool)

    def __init__(
        self,
        title: str,
        content: QtWidgets.QWidget,
        parent: QtWidgets.QWidget | None = None,
        *,
        collapsible: bool = False,
        expanded: bool = True,
        scrollable_body: bool = False,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("panelFrame")
        self.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Preferred)
        self._collapsible = bool(collapsible)
        self._expanded = bool(expanded)
        self._expanded_max_height_cap: int | None = None
        self._expanded_min_height = int(self.minimumHeight())
        self._expanded_max_height = int(self.maximumHeight())
        self._expanded_size_policy = QtWidgets.QSizePolicy(self.sizePolicy())
        self._keep_expanding_when_collapsed = False
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._header = QtWidgets.QWidget(self)
        self._header.setObjectName("panelFrameHeader")
        self._header.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Fixed,
        )
        header_layout = QtWidgets.QHBoxLayout(self._header)
        header_layout.setContentsMargins(12, 6, 12, 6)
        header_layout.setSpacing(6)
        self._title_label: QtWidgets.QLabel | None = None
        self._toggle_button: QtWidgets.QToolButton | None = None
        if self._collapsible:
            self._toggle_button = QtWidgets.QToolButton(self._header)
            self._toggle_button.setObjectName("panelFrameToggle")
            self._toggle_button.setFixedSize(14, 14)
            self._toggle_button.setCheckable(True)
            self._toggle_button.clicked.connect(self._on_toggled)
            header_layout.addWidget(self._toggle_button, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        else:
            header_layout.addSpacing(2)

        self._title_label = QtWidgets.QLabel(title, self._header)
        self._title_label.setObjectName("panelFrameTitle")
        header_layout.addWidget(self._title_label)
        header_layout.addStretch(1)

        if self._collapsible:
            self._title_label.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
            self._header.mousePressEvent = self._on_header_mouse_press
            self._title_label.mousePressEvent = self._on_header_mouse_press

        self._body = QtWidgets.QWidget(self)
        self._body.setObjectName("panelFrameBody")
        body_layout = QtWidgets.QVBoxLayout(self._body)
        right_gutter = styled_scrollbar_extent(DEFAULT_THEME) if bool(scrollable_body) else 8
        body_layout.setContentsMargins(8, 8, right_gutter, 8)
        body_layout.setSpacing(0)
        if bool(scrollable_body):
            self._body_scroll = QtWidgets.QScrollArea(self._body)
            self._body_scroll.setObjectName("panelFrameBodyScroll")
            self._body_scroll.setWidgetResizable(True)
            self._body_scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
            self._body_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
            self._body_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)
            self._body_scroll.setWidget(content)
            body_layout.addWidget(self._body_scroll)
        else:
            self._body_scroll = None
            body_layout.addWidget(content)
        self._body_layout = body_layout

        root.addWidget(self._header)
        root.addWidget(self._body, 1)
        self.set_expanded(self._expanded)

    def set_body_margins(self, left: int, top: int, right: int, bottom: int) -> None:
        self._body_layout.setContentsMargins(left, top, right, bottom)

    def set_expanded_max_height(self, height: int | None) -> None:
        if height is None:
            self._expanded_max_height_cap = None
        else:
            self._expanded_max_height_cap = max(1, int(height))
        if self._expanded:
            self.set_expanded(True)

    def set_expanded_min_height(self, height: int) -> None:
        self._expanded_min_height = max(0, int(height))
        if self._expanded:
            self.set_expanded(True)

    def set_expanded_size_policy(self, policy: QtWidgets.QSizePolicy) -> None:
        self._expanded_size_policy = QtWidgets.QSizePolicy(policy)
        if self._expanded:
            self.setSizePolicy(self._expanded_size_policy)

    def set_keep_expanding_when_collapsed(self, enabled: bool) -> None:
        self._keep_expanding_when_collapsed = bool(enabled)
        if not self._expanded:
            self.set_expanded(False)

    def _on_toggled(self, checked: bool) -> None:
        self.set_expanded(bool(checked))

    def _on_header_mouse_press(self, event: QtGui.QMouseEvent) -> None:  # type: ignore[override]
        if self._toggle_button is None:
            return
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            self.set_expanded(not self._expanded)
            event.accept()
            return
        event.ignore()

    def _collapsed_height(self) -> int:
        return max(18, int(self._header.sizeHint().height()) + 2)

    def collapsed_height_hint(self) -> int:
        return int(self._collapsed_height())

    def set_expanded(self, expanded: bool) -> None:
        new_expanded = bool(expanded)
        changed = new_expanded != self._expanded
        self._expanded = new_expanded
        if self._toggle_button is not None:
            self._toggle_button.blockSignals(True)
            self._toggle_button.setChecked(self._expanded)
            self._toggle_button.setArrowType(QtCore.Qt.ArrowType.DownArrow if self._expanded else QtCore.Qt.ArrowType.RightArrow)
            self._toggle_button.blockSignals(False)
        self._body.setVisible(self._expanded or (not self._collapsible))
        if self._collapsible:
            if self._expanded:
                expanded_min = max(
                    int(self._expanded_min_height),
                    int(self._collapsed_height()),
                    int(self.minimumSizeHint().height()),
                )
                self.setMinimumHeight(expanded_min)
                max_height = int(self._expanded_max_height)
                if self._expanded_max_height_cap is not None:
                    max_height = min(max_height, int(self._expanded_max_height_cap))
                    max_height = max(max_height, expanded_min)
                self.setMaximumHeight(max_height)
                self.setSizePolicy(self._expanded_size_policy)
            else:
                collapsed_height = self._collapsed_height()
                self.setMinimumHeight(collapsed_height)
                if self._keep_expanding_when_collapsed:
                    max_height = int(self._expanded_max_height)
                    if self._expanded_max_height_cap is not None:
                        max_height = min(max_height, int(self._expanded_max_height_cap))
                    self.setMaximumHeight(max_height)
                    self.setSizePolicy(self._expanded_size_policy)
                else:
                    collapsed_policy = QtWidgets.QSizePolicy(self._expanded_size_policy)
                    collapsed_policy.setVerticalPolicy(QtWidgets.QSizePolicy.Policy.Fixed)
                    self.setSizePolicy(collapsed_policy)
                    self.setMaximumHeight(collapsed_height)
        self.updateGeometry()
        if changed:
            self.expanded_changed.emit(self._expanded)

    def is_expanded(self) -> bool:
        return bool(self._expanded)

    def minimumSizeHint(self) -> QtCore.QSize:
        base = super().minimumSizeHint()
        if not self._collapsible:
            return base
        return QtCore.QSize(base.width(), max(base.height(), self._collapsed_height()))

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        if not self._collapsible or not self._expanded:
            return
        old_h = int(event.oldSize().height())
        new_h = int(event.size().height())
        threshold = int(self._collapsed_height()) + 4
        if old_h > threshold and new_h <= threshold:
            QtCore.QTimer.singleShot(0, lambda: self.set_expanded(False))


class CollapsibleSection(QtWidgets.QWidget):
    def __init__(self, title: str, content: QtWidgets.QWidget, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("collapsibleSection")
        self.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Maximum)
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)

        self.toggle_button = QtWidgets.QToolButton(self)
        self.toggle_button.setObjectName("collapsibleSectionHeader")
        self.toggle_button.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
        self.toggle_button.setText(title)
        self.toggle_button.setCheckable(True)
        self.toggle_button.setChecked(True)
        self.toggle_button.setToolButtonStyle(QtCore.Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.toggle_button.setArrowType(QtCore.Qt.ArrowType.DownArrow)
        self.toggle_button.clicked.connect(self._on_toggled)
        root.addWidget(self.toggle_button)

        self.content = content
        root.addWidget(content)

    def _on_toggled(self, checked: bool) -> None:
        self.toggle_button.setArrowType(QtCore.Qt.ArrowType.DownArrow if checked else QtCore.Qt.ArrowType.RightArrow)
        self.content.setVisible(checked)

    def setExpanded(self, expanded: bool) -> None:
        self.toggle_button.setChecked(bool(expanded))
        self._on_toggled(bool(expanded))


class ColorPickerButton(QtWidgets.QPushButton):
    color_changed = QtCore.Signal(str)

    def __init__(self, color_hex: str, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._color_hex = color_hex
        self.clicked.connect(self._pick_color)
        self._refresh_ui()

    def color_hex(self) -> str:
        return self._color_hex

    def set_color_hex(self, color_hex: str) -> None:
        c = QtGui.QColor(color_hex)
        if not c.isValid():
            return
        self._color_hex = c.name()
        self._refresh_ui()
        self.color_changed.emit(self._color_hex)

    def _refresh_ui(self) -> None:
        self.setText(self._color_hex.upper())
        fg = "#000000" if QtGui.QColor(self._color_hex).lightness() > 140 else "#ffffff"
        self.setStyleSheet(
            f"QPushButton {{ background-color: {self._color_hex}; color: {fg}; border: 1px solid #666; padding: 4px 8px; }}"
        )

    def _pick_color(self) -> None:
        chosen = QtWidgets.QColorDialog.getColor(QtGui.QColor(self._color_hex), self, "Choose Color")
        if chosen.isValid():
            self.set_color_hex(chosen.name())


class PreferencesDialog(QtWidgets.QDialog):
    applied = QtCore.Signal(dict)
    open_logs_requested = QtCore.Signal()
    clear_logs_requested = QtCore.Signal()

    def __init__(
        self,
        hbatch_path: str,
        player_path: str,
        theme: dict[str, str],
        startup_options: dict[str, Any],
        runtime_defaults: dict[str, Any],
        experimental_flags: dict[str, Any],
        device_defaults: dict[str, Any],
        available_render_devices: list[dict[str, str]],
        logs_dir: str,
        discover_hbatch_fn: Callable[[], str],
        safe_message_fn: Callable[[QtWidgets.QWidget, str, str, str | None], None],
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Preferences")
        self.resize(760, 620)
        self._discover_hbatch_fn = discover_hbatch_fn
        self._safe_message_fn = safe_message_fn
        self._theme_buttons: dict[str, ColorPickerButton] = {}
        self._logs_dir = Path(logs_dir)
        self._build_ui(
            hbatch_path,
            player_path,
            theme,
            startup_options,
            runtime_defaults,
            experimental_flags,
            device_defaults,
            available_render_devices,
            logs_dir,
        )

    def _build_ui(
        self,
        hbatch_path: str,
        player_path: str,
        theme: dict[str, str],
        startup_options: dict[str, Any],
        runtime_defaults: dict[str, Any],
        experimental_flags: dict[str, Any],
        device_defaults: dict[str, Any],
        available_render_devices: list[dict[str, str]],
        logs_dir: str,
    ) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        scroll = QtWidgets.QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOn)

        scroll_host = QtWidgets.QWidget()
        scroll_host.setObjectName("transparentHost")
        scroll_layout = QtWidgets.QVBoxLayout(scroll_host)
        scroll_layout.setContentsMargins(0, 0, 8, 0)
        scroll_layout.setSpacing(8)

        settings_layout = QtWidgets.QVBoxLayout()
        settings_layout.setContentsMargins(0, 0, 0, 0)
        settings_layout.setSpacing(6)

        metric_labels = [
            "Panel Gap",
            "Selection Overlay Opacity",
        ]
        runtime_labels = [
            "Chunking Enabled",
            "Chunk Size",
            "Retry Count",
            "Retry Delay (s)",
        ]
        startup_labels = [
            "Check Files On Startup",
            "Reload All Jobs On Startup",
        ]
        color_groups: list[list[tuple[str, str]]] = [
            [
                ("background", "Background"),
                ("panel_bg", "Panel Background"),
                ("text", "Text"),
                ("button_bg", "Buttons"),
                ("button_text", "Button Text"),
                ("input_bg", "Inputs"),
                ("input_text", "Input Text"),
                ("text_selection_bg", "Text Select BG"),
                ("text_selection_text", "Text Select Text"),
            ],
            [
                ("table_base", "Table Base"),
                ("table_alt", "Table Alternate"),
                ("selection_row", "Selection Row"),
                ("selection_row_alt", "Selection Row Alt"),
            ],
            [
                ("queue_running", "Queue Running"),
                ("queue_done", "Queue Done"),
                ("queue_failed", "Queue Failed"),
                ("lock_color", "Lock Color"),
                ("progress_usd_build", "USD Build Line"),
                ("progress_render", "Render Line"),
            ],
        ]
        all_labels = [label for group in color_groups for _, label in group] + metric_labels + runtime_labels + startup_labels
        label_width = max((self.fontMetrics().horizontalAdvance(label) for label in all_labels), default=0) + 12
        control_min_width = 120
        divider_width = 1

        def _configure_pref_grid(grid: QtWidgets.QGridLayout, row_count: int) -> None:
            grid.setContentsMargins(10, 10, 10, 10)
            grid.setHorizontalSpacing(10)
            grid.setVerticalSpacing(8)
            grid.setColumnStretch(0, 0)
            grid.setColumnStretch(1, 0)
            grid.setColumnStretch(2, 1)
            divider = QtWidgets.QFrame()
            divider.setObjectName("toolbarSeparator")
            divider.setFrameShape(QtWidgets.QFrame.Shape.VLine)
            divider.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
            divider.setFixedWidth(divider_width)
            grid.addWidget(divider, 0, 1, max(1, row_count), 1)

        hbatch_row = QtWidgets.QHBoxLayout()
        hbatch_row.setContentsMargins(0, 0, 0, 0)
        hbatch_row.setSpacing(8)
        self.hbatch_edit = QtWidgets.QLineEdit(hbatch_path)
        self.hbatch_edit.setPlaceholderText("Path to hbatch.exe")
        hbatch_row.addWidget(self.hbatch_edit, 1)
        self.btn_browse_hbatch = QtWidgets.QPushButton("Browse...")
        self.btn_browse_hbatch.clicked.connect(self._browse_hbatch)
        hbatch_row.addWidget(self.btn_browse_hbatch)
        self.btn_auto_find_hbatch = QtWidgets.QPushButton("Auto Find")
        self.btn_auto_find_hbatch.clicked.connect(self._auto_find_hbatch)
        hbatch_row.addWidget(self.btn_auto_find_hbatch)
        hbatch_row.addStretch(0)
        hbatch_row_host = QtWidgets.QWidget()
        hbatch_row_host.setObjectName("transparentHost")
        hbatch_row_host.setLayout(hbatch_row)
        settings_layout.addWidget(hbatch_row_host)

        status_row = QtWidgets.QHBoxLayout()
        status_row.setContentsMargins(0, 0, 0, 0)
        status_row.setSpacing(6)
        self.hbatch_status_prefix = QtWidgets.QLabel("Status:")
        status_row.addWidget(self.hbatch_status_prefix)
        self.hbatch_status_label = QtWidgets.QLabel()
        self.hbatch_status_label.setMinimumWidth(120)
        status_row.addWidget(self.hbatch_status_label)
        status_row.addStretch(1)
        status_row_host = QtWidgets.QWidget()
        status_row_host.setObjectName("transparentHost")
        status_row_host.setLayout(status_row)
        settings_layout.addWidget(status_row_host)
        self.hbatch_edit.textChanged.connect(self._update_hbatch_status_label)
        self._update_hbatch_status_label()
        settings_host = QtWidgets.QWidget()
        settings_host.setObjectName("transparentHost")
        settings_host.setLayout(settings_layout)
        settings_panel = PanelFrame("Houdini Headless Executable - hbatch.exe", settings_host)

        player_settings_layout = QtWidgets.QVBoxLayout()
        player_settings_layout.setContentsMargins(0, 0, 0, 0)
        player_settings_layout.setSpacing(6)
        player_row = QtWidgets.QHBoxLayout()
        player_row.setContentsMargins(0, 0, 0, 0)
        player_row.setSpacing(8)
        self.player_edit = QtWidgets.QLineEdit(player_path)
        self.player_edit.setPlaceholderText("Path to player executable")
        player_row.addWidget(self.player_edit, 1)
        self.btn_browse_player = QtWidgets.QPushButton("Browse...")
        self.btn_browse_player.clicked.connect(self._browse_player)
        player_row.addWidget(self.btn_browse_player)
        player_row.addStretch(0)
        player_row_host = QtWidgets.QWidget()
        player_row_host.setObjectName("transparentHost")
        player_row_host.setLayout(player_row)
        player_settings_layout.addWidget(player_row_host)
        player_status_row = QtWidgets.QHBoxLayout()
        player_status_row.setContentsMargins(0, 0, 0, 0)
        player_status_row.setSpacing(6)
        self.player_status_prefix = QtWidgets.QLabel("Status:")
        player_status_row.addWidget(self.player_status_prefix)
        self.player_status_label = QtWidgets.QLabel()
        self.player_status_label.setMinimumWidth(120)
        player_status_row.addWidget(self.player_status_label)
        player_status_row.addStretch(1)
        player_status_row_host = QtWidgets.QWidget()
        player_status_row_host.setObjectName("transparentHost")
        player_status_row_host.setLayout(player_status_row)
        player_settings_layout.addWidget(player_status_row_host)
        self.player_edit.textChanged.connect(self._update_player_status_label)
        self._update_player_status_label()
        player_settings_host = QtWidgets.QWidget()
        player_settings_host.setObjectName("transparentHost")
        player_settings_host.setLayout(player_settings_layout)
        player_settings_panel = PanelFrame("Preview Player", player_settings_host)

        runtime_host = QtWidgets.QWidget()
        runtime_host.setObjectName("transparentHost")
        runtime_layout = QtWidgets.QGridLayout(runtime_host)
        _configure_pref_grid(runtime_layout, 4)
        self.chk_default_chunking_enabled = QtWidgets.QCheckBox("Enabled")
        self.chk_default_chunking_enabled.setChecked(bool(runtime_defaults.get("chunking_enabled", False)))
        self.spin_default_chunk_size = CleanStepSpinBox()
        self.spin_default_chunk_size.setRange(1, 100000)
        self.spin_default_chunk_size.setValue(max(1, int(runtime_defaults.get("chunk_size", 10))))
        self.spin_default_chunk_size.setMinimumWidth(control_min_width)
        self.spin_default_chunk_size.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Fixed,
        )
        self.spin_default_retry_count = CleanStepSpinBox()
        self.spin_default_retry_count.setRange(0, 20)
        self.spin_default_retry_count.setValue(max(0, int(runtime_defaults.get("retry_count", 1))))
        self.spin_default_retry_count.setMinimumWidth(control_min_width)
        self.spin_default_retry_count.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Fixed,
        )
        self.spin_default_retry_delay = CleanStepSpinBox()
        self.spin_default_retry_delay.setRange(0, 3600)
        self.spin_default_retry_delay.setValue(max(0, int(runtime_defaults.get("retry_delay", 5))))
        self.spin_default_retry_delay.setMinimumWidth(control_min_width)
        self.spin_default_retry_delay.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Fixed,
        )

        def _runtime_label(text: str) -> QtWidgets.QLabel:
            label = QtWidgets.QLabel(text)
            label.setObjectName("parameterLabel")
            label.setFixedWidth(label_width)
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
            return label

        self.lbl_default_chunking_enabled = _runtime_label("Chunking Enabled")
        runtime_layout.addWidget(self.lbl_default_chunking_enabled, 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        runtime_layout.addWidget(self.chk_default_chunking_enabled, 0, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        self.lbl_default_chunk_size = _runtime_label("Chunk Size")
        runtime_layout.addWidget(self.lbl_default_chunk_size, 1, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        runtime_layout.addWidget(self.spin_default_chunk_size, 1, 2)
        runtime_layout.addWidget(_runtime_label("Retry Count"), 2, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        runtime_layout.addWidget(self.spin_default_retry_count, 2, 2)
        runtime_layout.addWidget(_runtime_label("Retry Delay (s)"), 3, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        runtime_layout.addWidget(self.spin_default_retry_delay, 3, 2)
        runtime_panel = PanelFrame("Queue Runtime Defaults", runtime_host)

        experimental_host = QtWidgets.QWidget()
        experimental_host.setObjectName("transparentHost")
        experimental_layout = QtWidgets.QGridLayout(experimental_host)
        _configure_pref_grid(experimental_layout, 1)
        self.chk_experimental_chunking = QtWidgets.QCheckBox("Enabled")
        self.chk_experimental_chunking.setChecked(bool(experimental_flags.get("chunking", False)))
        self.chk_experimental_chunking.toggled.connect(self._refresh_experimental_ui)
        experimental_label = QtWidgets.QLabel("Chunking")
        experimental_label.setObjectName("parameterLabel")
        experimental_label.setFixedWidth(label_width)
        experimental_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
        experimental_layout.addWidget(experimental_label, 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        experimental_layout.addWidget(self.chk_experimental_chunking, 0, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        experimental_panel = PanelFrame("Experimental", experimental_host)

        startup_host = QtWidgets.QWidget()
        startup_host.setObjectName("transparentHost")
        startup_layout = QtWidgets.QGridLayout(startup_host)
        _configure_pref_grid(startup_layout, 2)
        self.chk_startup_check_files = QtWidgets.QCheckBox("Enabled")
        self.chk_startup_check_files.setChecked(bool(startup_options.get("check_files_on_startup", True)))
        self.chk_startup_reload_all = QtWidgets.QCheckBox("Enabled")
        self.chk_startup_reload_all.setChecked(bool(startup_options.get("reload_all_jobs_on_startup", True)))
        startup_layout.addWidget(_runtime_label("Check Files On Startup"), 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        startup_layout.addWidget(self.chk_startup_check_files, 0, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        startup_layout.addWidget(_runtime_label("Reload All Jobs On Startup"), 1, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        startup_layout.addWidget(self.chk_startup_reload_all, 1, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        startup_panel = PanelFrame("Startup", startup_host)

        device_host = QtWidgets.QWidget()
        device_host.setObjectName("transparentHost")
        device_layout = QtWidgets.QGridLayout(device_host)
        _configure_pref_grid(device_layout, 3)
        self.default_device_mode = QtWidgets.QComboBox()
        for mode in (
            DeviceOverrideMode.DEFAULT,
            DeviceOverrideMode.CPU,
            DeviceOverrideMode.ALL_GPUS,
            DeviceOverrideMode.SPECIFIC_GPUS,
        ):
            self.default_device_mode.addItem(mode.label(), mode.value)
        current_default_mode = DeviceOverrideMode.coerce(device_defaults.get("mode"))
        self.default_device_mode.setCurrentIndex(max(0, self.default_device_mode.findData(current_default_mode.value)))
        self._default_device_option_checks: list[tuple[str, QtWidgets.QCheckBox]] = []
        self.default_device_selection_label = _runtime_label("Custom Devices")
        self.default_device_selection_container = QtWidgets.QWidget()
        self.default_device_selection_container.setObjectName("transparentHost")
        self.default_device_selection_layout = QtWidgets.QVBoxLayout(self.default_device_selection_container)
        self.default_device_selection_layout.setContentsMargins(0, 0, 0, 0)
        self.default_device_selection_layout.setSpacing(6)
        self._set_default_device_options(
            available_render_devices,
            selection=str(device_defaults.get("selection", "") or ""),
        )
        self.chk_default_retain_built_usd = QtWidgets.QCheckBox("Enabled")
        self.chk_default_retain_built_usd.setChecked(bool(device_defaults.get("retain_built_usd", False)))
        device_layout.addWidget(_runtime_label("Default Device"), 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        device_layout.addWidget(self.default_device_mode, 0, 2)
        device_layout.addWidget(self.default_device_selection_label, 1, 0, alignment=QtCore.Qt.AlignmentFlag.AlignTop)
        device_layout.addWidget(self.default_device_selection_container, 1, 2)
        device_panel = PanelFrame("Render Device Defaults", device_host)

        usd_defaults_host = QtWidgets.QWidget()
        usd_defaults_host.setObjectName("transparentHost")
        usd_defaults_layout = QtWidgets.QGridLayout(usd_defaults_host)
        _configure_pref_grid(usd_defaults_layout, 3)
        self.default_usd_output_mode = QtWidgets.QComboBox()
        for mode in (
            UsdOutputDirectoryMode.DEFAULT_TEMP,
            UsdOutputDirectoryMode.PROJECT_PATH,
            UsdOutputDirectoryMode.CUSTOM_PATH,
        ):
            self.default_usd_output_mode.addItem(mode.label(), mode.value)
        current_usd_output_mode = UsdOutputDirectoryMode.coerce(device_defaults.get("usd_output_directory_mode"))
        self.default_usd_output_mode.setCurrentIndex(max(0, self.default_usd_output_mode.findData(current_usd_output_mode.value)))
        self.default_usd_output_custom_path = QtWidgets.QLineEdit(str(device_defaults.get("usd_output_directory_custom_path", "") or ""))
        self.default_usd_output_custom_path.setPlaceholderText("Folder path")
        self.default_usd_output_custom_path.setMinimumWidth(control_min_width)
        self.btn_browse_default_usd_output_custom_path = QtWidgets.QPushButton("Browse")
        self.btn_browse_default_usd_output_custom_path.clicked.connect(self._browse_default_usd_output_custom_path)
        self.default_device_mode.currentIndexChanged.connect(self._refresh_device_defaults_ui)
        self.default_usd_output_mode.currentIndexChanged.connect(self._refresh_device_defaults_ui)
        usd_defaults_layout.addWidget(_runtime_label("Retain Built USD"), 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        usd_defaults_layout.addWidget(self.chk_default_retain_built_usd, 0, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        usd_defaults_layout.addWidget(_runtime_label("USD Output Directory"), 1, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        usd_defaults_layout.addWidget(self.default_usd_output_mode, 1, 2)
        usd_defaults_layout.addWidget(_runtime_label("USD Custom Path"), 2, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        default_usd_output_row = QtWidgets.QHBoxLayout()
        default_usd_output_row.setContentsMargins(0, 0, 0, 0)
        default_usd_output_row.setSpacing(8)
        default_usd_output_row.addWidget(self.default_usd_output_custom_path, 1)
        default_usd_output_row.addWidget(self.btn_browse_default_usd_output_custom_path)
        usd_defaults_layout.addLayout(default_usd_output_row, 2, 2)
        self._refresh_device_defaults_ui()
        usd_defaults_panel = PanelFrame("USD Defaults", usd_defaults_host)

        logs_host = QtWidgets.QWidget()
        logs_host.setObjectName("transparentHost")
        logs_layout = QtWidgets.QVBoxLayout(logs_host)
        logs_layout.setContentsMargins(10, 10, 10, 10)
        logs_layout.setSpacing(8)
        logs_path_label = QtWidgets.QLabel("Folder")
        logs_path_label.setObjectName("parameterLabel")
        self.logs_dir_value = QtWidgets.QLineEdit(logs_dir)
        self.logs_dir_value.setReadOnly(True)
        self.logs_dir_value.setMinimumWidth(control_min_width)
        logs_path_row = QtWidgets.QHBoxLayout()
        logs_path_row.setContentsMargins(0, 0, 0, 0)
        logs_path_row.setSpacing(10)
        logs_path_row.addWidget(logs_path_label)
        logs_path_row.addWidget(self.logs_dir_value, 1)
        logs_layout.addLayout(logs_path_row)
        self.logs_summary_label = QtWidgets.QLabel()
        self.logs_summary_label.setWordWrap(True)
        logs_layout.addWidget(self.logs_summary_label)
        logs_buttons = QtWidgets.QHBoxLayout()
        logs_buttons.setContentsMargins(0, 0, 0, 0)
        logs_buttons.setSpacing(8)
        self.btn_open_logs_folder = QtWidgets.QPushButton("Open Folder")
        self.btn_open_logs_folder.clicked.connect(self.open_logs_requested.emit)
        logs_buttons.addWidget(self.btn_open_logs_folder)
        self.btn_clear_logs = QtWidgets.QPushButton("Delete Log Files")
        self.btn_clear_logs.clicked.connect(self.clear_logs_requested.emit)
        logs_buttons.addWidget(self.btn_clear_logs)
        logs_buttons.addStretch(1)
        logs_layout.addLayout(logs_buttons)
        self.refresh_logs_summary()
        logs_panel = PanelFrame("Logs", logs_host)

        theme_host = QtWidgets.QWidget()
        theme_host.setObjectName("transparentHost")
        colors_root = QtWidgets.QVBoxLayout(theme_host)
        colors_root.setContentsMargins(0, 0, 0, 0)
        colors_root.setSpacing(8)
        def _add_color_group(title: str, fields: list[tuple[str, str]]) -> None:
            grp = QtWidgets.QGroupBox(title)
            gl = QtWidgets.QGridLayout(grp)
            _configure_pref_grid(gl, len(fields))
            for row, (key, label) in enumerate(fields):
                lbl = QtWidgets.QLabel(label)
                lbl.setObjectName("parameterLabel")
                lbl.setFixedWidth(label_width)
                lbl.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
                gl.addWidget(lbl, row, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
                btn = ColorPickerButton(str(theme.get(key, DEFAULT_THEME[key])))
                btn.setMinimumWidth(control_min_width)
                btn.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
                self._theme_buttons[key] = btn
                gl.addWidget(btn, row, 2)
            colors_root.addWidget(grp)

        _add_color_group(
            "General",
            color_groups[0],
        )
        _add_color_group(
            "Table / Selection",
            color_groups[1],
        )
        _add_color_group(
            "Queue / Indicators",
            color_groups[2],
        )

        metrics_box = QtWidgets.QGroupBox("Theme Layout")
        metrics_grid = QtWidgets.QGridLayout(metrics_box)
        _configure_pref_grid(metrics_grid, 3)
        self.panel_gap_spin = CleanStepSpinBox()
        self.panel_gap_spin.setRange(2, 24)
        self.panel_gap_spin.setValue(int(theme.get("panel_gap", DEFAULT_THEME["panel_gap"])))
        self.panel_gap_spin.setMinimumWidth(control_min_width)
        self.panel_gap_spin.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
        panel_gap_label = QtWidgets.QLabel("Panel Gap")
        panel_gap_label.setObjectName("parameterLabel")
        panel_gap_label.setFixedWidth(label_width)
        panel_gap_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
        metrics_grid.addWidget(panel_gap_label, 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        metrics_grid.addWidget(self.panel_gap_spin, 0, 2)
        self.selection_overlay_opacity_spin = CleanStepSpinBox()
        self.selection_overlay_opacity_spin.setRange(0, 255)
        self.selection_overlay_opacity_spin.setValue(
            int(theme.get("selection_overlay_opacity", DEFAULT_THEME["selection_overlay_opacity"]))
        )
        self.selection_overlay_opacity_spin.setMinimumWidth(control_min_width)
        self.selection_overlay_opacity_spin.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
        overlay_label = QtWidgets.QLabel("Selection Overlay Opacity")
        overlay_label.setObjectName("parameterLabel")
        overlay_label.setFixedWidth(label_width)
        overlay_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
        metrics_grid.addWidget(overlay_label, 1, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        metrics_grid.addWidget(self.selection_overlay_opacity_spin, 1, 2)
        self.path_sync_overlay_opacity_spin = CleanStepSpinBox()
        self.path_sync_overlay_opacity_spin.setRange(0, 255)
        self.path_sync_overlay_opacity_spin.setValue(
            int(theme.get("path_sync_overlay_opacity", DEFAULT_THEME["path_sync_overlay_opacity"]))
        )
        self.path_sync_overlay_opacity_spin.setMinimumWidth(control_min_width)
        self.path_sync_overlay_opacity_spin.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed
        )
        path_sync_overlay_label = QtWidgets.QLabel("Path Sync Overlay Opacity")
        path_sync_overlay_label.setObjectName("parameterLabel")
        path_sync_overlay_label.setFixedWidth(label_width)
        path_sync_overlay_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter)
        metrics_grid.addWidget(path_sync_overlay_label, 2, 0, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        metrics_grid.addWidget(self.path_sync_overlay_opacity_spin, 2, 2)
        colors_root.addWidget(metrics_box)
        buttons = QtWidgets.QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)
        buttons.addStretch(1)
        self.btn_reset_theme = QtWidgets.QPushButton("Reset Theme")
        self.btn_reset_theme.clicked.connect(self._reset_theme)
        buttons.addWidget(self.btn_reset_theme)
        self.btn_apply = QtWidgets.QPushButton("Apply")
        self.btn_apply.clicked.connect(self._emit_apply)
        buttons.addWidget(self.btn_apply)
        self.btn_close = QtWidgets.QPushButton("Close")
        self.btn_close.clicked.connect(self.accept)
        buttons.addWidget(self.btn_close)
        buttons_host = QtWidgets.QWidget()
        buttons_host.setObjectName("transparentHost")
        buttons_host.setLayout(buttons)
        colors_root.addStretch(1)
        colors_root.addWidget(buttons_host)
        theme_panel = PanelFrame("Theme", theme_host)

        def _configure_preferences_rich_text_label(label: QtWidgets.QLabel) -> None:
            label.setWordWrap(True)
            label.setOpenExternalLinks(True)
            label.setTextFormat(QtCore.Qt.TextFormat.RichText)
            label.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextBrowserInteraction)
            palette = label.palette()
            palette.setColor(QtGui.QPalette.ColorRole.WindowText, QtGui.QColor("#d8d8d8"))
            label.setPalette(palette)
            label.setStyleSheet("QLabel { color: #d8d8d8; }")

        creator_host = QtWidgets.QWidget()
        creator_host.setObjectName("transparentHost")
        creator_layout = QtWidgets.QVBoxLayout(creator_host)
        creator_layout.setContentsMargins(10, 10, 10, 10)
        creator_layout.setSpacing(8)
        creator_notice = QtWidgets.QLabel(CREATOR_NOTICE_HTML)
        _configure_preferences_rich_text_label(creator_notice)
        creator_layout.addWidget(creator_notice)
        creator_panel = PanelFrame("Creator", creator_host)

        third_party_host = QtWidgets.QWidget()
        third_party_host.setObjectName("transparentHost")
        third_party_layout = QtWidgets.QVBoxLayout(third_party_host)
        third_party_layout.setContentsMargins(10, 10, 10, 10)
        third_party_layout.setSpacing(8)
        third_party_notice = QtWidgets.QLabel(TABLER_NOTIFICATION_NOTICE_HTML)
        _configure_preferences_rich_text_label(third_party_notice)
        third_party_layout.addWidget(third_party_notice)
        third_party_panel = PanelFrame("Third-Party Assets", third_party_host)

        scroll_layout.addWidget(settings_panel)
        scroll_layout.addWidget(player_settings_panel)
        scroll_layout.addWidget(experimental_panel)
        scroll_layout.addWidget(startup_panel)
        scroll_layout.addWidget(runtime_panel)
        scroll_layout.addWidget(device_panel)
        scroll_layout.addWidget(usd_defaults_panel)
        scroll_layout.addWidget(logs_panel)
        scroll_layout.addWidget(theme_panel)
        scroll_layout.addWidget(creator_panel)
        scroll_layout.addWidget(third_party_panel)
        scroll_layout.addStretch(1)

        scroll.setWidget(scroll_host)
        root.addWidget(scroll, 1)
        self._refresh_experimental_ui()

    def _browse_hbatch(self) -> None:
        current = self.hbatch_edit.text().strip()
        start_dir = str(Path(current).parent) if current else r"C:\Program Files\Side Effects Software"
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Select hbatch.exe",
            start_dir,
            "hbatch.exe (hbatch.exe);;Executables (*.exe);;All Files (*.*)",
        )
        if path:
            self.hbatch_edit.setText(path)

    def _auto_find_hbatch(self) -> None:
        found = self._discover_hbatch_fn()
        if found:
            self.hbatch_edit.setText(found)
        else:
            self._safe_message_fn(self, "hbatch Not Found", "Could not auto-discover hbatch.exe.", None)

    def _browse_player(self) -> None:
        current = self.player_edit.text().strip()
        start_dir = str(Path(current).parent) if current else str(Path.home())
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Select Player Executable",
            start_dir,
            "Executables (*.exe);;All Files (*.*)",
        )
        if path:
            self.player_edit.setText(path)

    def _browse_default_usd_output_custom_path(self) -> None:
        current = self.default_usd_output_custom_path.text().strip()
        start_dir = current if current else str(Path.home())
        path = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Select USD Output Folder",
            start_dir,
        )
        if path:
            self.default_usd_output_custom_path.setText(path)

    def _update_hbatch_status_label(self) -> None:
        path = self.hbatch_edit.text().strip()
        if path and Path(path).exists():
            self.hbatch_status_label.setText("<span style='color:#2e7d32; font-weight:600;'>Exists</span>")
        elif path:
            self.hbatch_status_label.setText("<span style='color:#b71c1c; font-weight:600;'>Missing</span>")
        else:
            self.hbatch_status_label.setText("<span style='color:#b71c1c; font-weight:600;'>Not Configured</span>")

    def _update_player_status_label(self) -> None:
        path = self.player_edit.text().strip()
        if path and Path(path).exists():
            self.player_status_label.setText("<span style='color:#2e7d32; font-weight:600;'>Exists</span>")
        elif path:
            self.player_status_label.setText("<span style='color:#b71c1c; font-weight:600;'>Missing</span>")
        else:
            self.player_status_label.setText("<span style='color:#b71c1c; font-weight:600;'>Not Configured</span>")

    def _reset_theme(self) -> None:
        for key, btn in self._theme_buttons.items():
            btn.set_color_hex(DEFAULT_THEME[key])
        self.panel_gap_spin.setValue(int(DEFAULT_THEME.get("panel_gap", 6)))
        self.selection_overlay_opacity_spin.setValue(int(DEFAULT_THEME.get("selection_overlay_opacity", 95)))
        self.path_sync_overlay_opacity_spin.setValue(int(DEFAULT_THEME.get("path_sync_overlay_opacity", 28)))

    def refresh_logs_summary(self) -> None:
        try:
            self._logs_dir.mkdir(parents=True, exist_ok=True)
            log_paths = [p for p in self._logs_dir.glob("*.log") if p.is_file()]
        except Exception as exc:
            self.logs_summary_label.setText(f"Summary unavailable: {exc}")
            return
        total_bytes = 0
        for path in log_paths:
            try:
                total_bytes += path.stat().st_size
            except Exception:
                continue
        self.logs_summary_label.setText(
            f"{len(log_paths)} log file(s) in folder, total size {self._format_bytes(total_bytes)}."
        )

    @staticmethod
    def _format_bytes(size: int) -> str:
        if size <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        power = min(int(math.log(size, 1024)), len(units) - 1)
        scaled = size / (1024 ** power)
        if power == 0:
            return f"{int(scaled)} {units[power]}"
        return f"{scaled:.1f} {units[power]}"

    def _emit_apply(self) -> None:
        self.applied.emit(self.values())

    def _refresh_device_defaults_ui(self) -> None:
        mode = DeviceOverrideMode.coerce(self.default_device_mode.currentData())
        show_custom_devices = mode is DeviceOverrideMode.SPECIFIC_GPUS
        self.default_device_selection_label.setVisible(show_custom_devices)
        self.default_device_selection_container.setVisible(show_custom_devices)
        for _device_id, checkbox in self._default_device_option_checks:
            checkbox.setEnabled(show_custom_devices)
        usd_mode = UsdOutputDirectoryMode.coerce(self.default_usd_output_mode.currentData())
        enable_custom_usd_path = usd_mode is UsdOutputDirectoryMode.CUSTOM_PATH
        self.default_usd_output_custom_path.setEnabled(enable_custom_usd_path)
        self.btn_browse_default_usd_output_custom_path.setEnabled(enable_custom_usd_path)

    def _set_default_device_options(self, devices: list[dict[str, str]], *, selection: str) -> None:
        normalized = RenderJob.normalize_device_selection(selection)
        selected_ids = set(part for part in normalized.split(",") if part.isdigit())
        while self.default_device_selection_layout.count():
            item = self.default_device_selection_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._default_device_option_checks = []
        if not devices:
            empty = QtWidgets.QLabel("No render devices detected")
            empty.setWordWrap(True)
            self.default_device_selection_layout.addWidget(empty)
            return
        for device in devices:
            device_id = str(device.get("id", "") or "")
            checkbox = QtWidgets.QCheckBox(str(device.get("name", "") or device_id))
            checkbox.setChecked(device_id in selected_ids)
            self._default_device_option_checks.append((device_id, checkbox))
            self.default_device_selection_layout.addWidget(checkbox)
        self.default_device_selection_layout.addStretch(1)

    def _refresh_experimental_ui(self) -> None:
        chunking_enabled = bool(self.chk_experimental_chunking.isChecked())
        for widget in (
            getattr(self, "lbl_default_chunking_enabled", None),
            getattr(self, "chk_default_chunking_enabled", None),
            getattr(self, "lbl_default_chunk_size", None),
            getattr(self, "spin_default_chunk_size", None),
        ):
            if widget is not None:
                widget.setVisible(chunking_enabled)

    def values(self) -> dict[str, Any]:
        theme = {key: btn.color_hex() for key, btn in self._theme_buttons.items()}
        theme["panel_gap"] = int(self.panel_gap_spin.value())
        theme["selection_overlay_opacity"] = int(self.selection_overlay_opacity_spin.value())
        theme["path_sync_overlay_opacity"] = int(self.path_sync_overlay_opacity_spin.value())
        return {
            "hbatch_path": self.hbatch_edit.text().strip(),
            "player_path": self.player_edit.text().strip(),
            "runtime_defaults": {
                "chunking_enabled": bool(self.chk_default_chunking_enabled.isChecked()),
                "chunk_size": int(self.spin_default_chunk_size.value()),
                "retry_count": int(self.spin_default_retry_count.value()),
                "retry_delay": int(self.spin_default_retry_delay.value()),
            },
            "experimental_flags": {
                "chunking": bool(self.chk_experimental_chunking.isChecked()),
            },
            "startup_options": {
                "check_files_on_startup": bool(self.chk_startup_check_files.isChecked()),
                "reload_all_jobs_on_startup": bool(self.chk_startup_reload_all.isChecked()),
            },
            "device_defaults": {
                "mode": str(self.default_device_mode.currentData() or DeviceOverrideMode.DEFAULT.value),
                "selection": ",".join(
                    device_id
                    for device_id, checkbox in self._default_device_option_checks
                    if checkbox.isChecked()
                ),
                "retain_built_usd": bool(self.chk_default_retain_built_usd.isChecked()),
                "usd_output_directory_mode": str(self.default_usd_output_mode.currentData() or UsdOutputDirectoryMode.DEFAULT_TEMP.value),
                "usd_output_directory_custom_path": self.default_usd_output_custom_path.text().strip(),
            },
            "theme": theme,
        }


class JobPropertiesPanel(QtWidgets.QWidget):
    device_mode_changed = QtCore.Signal(str)
    device_selection_changed = QtCore.Signal(str)
    render_all_frames_single_process_changed = QtCore.Signal(bool)
    retain_built_usd_changed = QtCore.Signal(bool)
    reuse_retained_usd_changed = QtCore.Signal(bool)
    usd_output_directory_mode_changed = QtCore.Signal(str)
    usd_output_directory_custom_path_changed = QtCore.Signal(str)
    reveal_retained_usd_requested = QtCore.Signal()
    delete_retained_usd_requested = QtCore.Signal()

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._syncing = False
        self._device_option_checks: list[tuple[str, QtWidgets.QCheckBox]] = []
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        def _label(text: str) -> QtWidgets.QLabel:
            label = QtWidgets.QLabel(text)
            label.setObjectName("parameterLabel")
            return label

        def _hline() -> QtWidgets.QFrame:
            line = QtWidgets.QFrame()
            line.setFrameShape(QtWidgets.QFrame.Shape.HLine)
            line.setFrameShadow(QtWidgets.QFrame.Shadow.Plain)
            line.setObjectName("jobPropertiesSeparator")
            line.setFixedHeight(1)
            return line

        def _value_label() -> QtWidgets.QLabel:
            label = QtWidgets.QLabel("-")
            label.setWordWrap(True)
            label.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
            return label

        info_grid = QtWidgets.QGridLayout()
        info_grid.setContentsMargins(12, 10, 8, 0)
        info_grid.setHorizontalSpacing(10)
        info_grid.setVerticalSpacing(8)
        info_grid.setColumnStretch(1, 1)
        layout.addLayout(info_grid)

        row = 0
        info_grid.addWidget(_label("Name"), row, 0)
        self.name_value = _value_label()
        info_grid.addWidget(self.name_value, row, 1)
        row += 1

        info_grid.addWidget(_label("File"), row, 0)
        self.file_value = _value_label()
        info_grid.addWidget(self.file_value, row, 1)
        row += 1

        info_grid.addWidget(_label("ROP"), row, 0)
        self.rop_value = _value_label()
        info_grid.addWidget(self.rop_value, row, 1)

        usd_host = QtWidgets.QWidget()
        usd_host.setObjectName("transparentHost")
        retain_grid = QtWidgets.QGridLayout(usd_host)
        retain_grid.setContentsMargins(12, 0, 8, 0)
        retain_grid.setHorizontalSpacing(10)
        retain_grid.setVerticalSpacing(8)
        retain_grid.setColumnStretch(1, 1)

        row = 0
        self.single_process_render_check = QtWidgets.QCheckBox("Render All Frames with a Single Process")
        self.single_process_render_check.setTristate(False)
        self.single_process_render_check.clicked.connect(self._emit_single_process_render_clicked)
        retain_grid.addWidget(self.single_process_render_check, row, 0, 1, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        row += 1

        self.retain_usd_check = QtWidgets.QCheckBox("Keep USD files")
        self.retain_usd_check.setTristate(False)
        self.retain_usd_check.clicked.connect(self._emit_retain_usd_clicked)
        retain_grid.addWidget(self.retain_usd_check, row, 0, 1, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        row += 1

        self.reuse_usd_check = QtWidgets.QCheckBox("Use existing USD files")
        self.reuse_usd_check.setTristate(False)
        self.reuse_usd_check.clicked.connect(self._emit_reuse_usd_clicked)
        retain_grid.addWidget(self.reuse_usd_check, row, 0, 1, 2, alignment=QtCore.Qt.AlignmentFlag.AlignLeft)
        row += 1

        retain_grid.addWidget(_label("USD Output Directory"), row, 0)
        self.usd_output_mode_combo = QtWidgets.QComboBox()
        self.usd_output_mode_combo.addItem(UsdOutputDirectoryMode.DEFAULT_TEMP.label(), UsdOutputDirectoryMode.DEFAULT_TEMP.value)
        self.usd_output_mode_combo.addItem(UsdOutputDirectoryMode.PROJECT_PATH.label(), UsdOutputDirectoryMode.PROJECT_PATH.value)
        self.usd_output_mode_combo.addItem(UsdOutputDirectoryMode.CUSTOM_PATH.label(), UsdOutputDirectoryMode.CUSTOM_PATH.value)
        if hasattr(self.usd_output_mode_combo, "setPlaceholderText"):
            self.usd_output_mode_combo.setPlaceholderText("Mixed")
        self.usd_output_mode_combo.currentIndexChanged.connect(self._emit_usd_output_directory_mode_changed)
        retain_grid.addWidget(self.usd_output_mode_combo, row, 1)
        row += 1

        self.usd_output_custom_path_label = _label("Custom USD Path")
        retain_grid.addWidget(self.usd_output_custom_path_label, row, 0)
        usd_output_custom_path_row = QtWidgets.QHBoxLayout()
        usd_output_custom_path_row.setContentsMargins(0, 0, 0, 0)
        usd_output_custom_path_row.setSpacing(8)
        self.usd_output_custom_path_value = QtWidgets.QLineEdit()
        self.usd_output_custom_path_value.setPlaceholderText("Folder path")
        self.usd_output_custom_path_value.editingFinished.connect(self._emit_usd_output_directory_custom_path_changed)
        usd_output_custom_path_row.addWidget(self.usd_output_custom_path_value, 1)
        self.btn_browse_usd_output_custom_path = QtWidgets.QPushButton("Browse")
        self.btn_browse_usd_output_custom_path.clicked.connect(self._browse_usd_output_custom_path)
        usd_output_custom_path_row.addWidget(self.btn_browse_usd_output_custom_path)
        self.usd_output_custom_path_host = QtWidgets.QWidget()
        self.usd_output_custom_path_host.setObjectName("transparentHost")
        self.usd_output_custom_path_host.setLayout(usd_output_custom_path_row)
        retain_grid.addWidget(self.usd_output_custom_path_host, row, 1)
        row += 1

        retain_grid.addWidget(_hline(), row, 0, 1, 2)
        row += 1

        retain_grid.addWidget(_label("Status"), row, 0)
        self.retained_usd_status = QtWidgets.QLabel("-")
        self.retained_usd_status.setWordWrap(True)
        retain_grid.addWidget(self.retained_usd_status, row, 1)
        row += 1

        self.retained_usd_warning_label = _label("Warning")
        self.retained_usd_warning_label.setVisible(False)
        retain_grid.addWidget(self.retained_usd_warning_label, row, 0, alignment=QtCore.Qt.AlignmentFlag.AlignTop)
        self.retained_usd_warning = QtWidgets.QLabel("")
        self.retained_usd_warning.setWordWrap(True)
        self.retained_usd_warning.setVisible(False)
        retain_grid.addWidget(self.retained_usd_warning, row, 1)
        row += 1

        retain_grid.addWidget(_label("Frame Range"), row, 0)
        self.retained_usd_built_range = QtWidgets.QLabel("-")
        self.retained_usd_built_range.setWordWrap(True)
        retain_grid.addWidget(self.retained_usd_built_range, row, 1)
        row += 1

        retain_grid.addWidget(_label("Step"), row, 0)
        self.retained_usd_built_step = QtWidgets.QLabel("-")
        self.retained_usd_built_step.setWordWrap(True)
        retain_grid.addWidget(self.retained_usd_built_step, row, 1)
        row += 1

        retain_grid.addWidget(_label("Built At"), row, 0)
        self.retained_usd_built_at = QtWidgets.QLabel("-")
        self.retained_usd_built_at.setWordWrap(True)
        retain_grid.addWidget(self.retained_usd_built_at, row, 1)
        row += 1

        retain_grid.addWidget(_label("USD Folder"), row, 0)
        path_row = QtWidgets.QHBoxLayout()
        path_row.setContentsMargins(0, 0, 0, 0)
        path_row.setSpacing(8)
        self.retained_usd_value = QtWidgets.QLineEdit()
        self.retained_usd_value.setReadOnly(True)
        self.retained_usd_value.setObjectName("jobPropertiesReadOnlyField")
        path_row.addWidget(self.retained_usd_value, 1)
        self.btn_reveal_retained_usd = QtWidgets.QPushButton("Open Folder")
        self.btn_reveal_retained_usd.clicked.connect(self.reveal_retained_usd_requested.emit)
        path_row.addWidget(self.btn_reveal_retained_usd)
        retain_grid.addLayout(path_row, row, 1)
        row += 1

        actions = QtWidgets.QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(8)
        self.btn_delete_retained_usd = QtWidgets.QPushButton("Delete")
        self.btn_delete_retained_usd.clicked.connect(self.delete_retained_usd_requested.emit)
        actions.addWidget(self.btn_delete_retained_usd)
        actions.addStretch(1)
        retain_grid.addLayout(actions, row, 1)

        self.usd_section = CollapsibleSection("USD", usd_host, self)
        self.usd_section.setExpanded(True)
        layout.addWidget(self.usd_section)

        device_host = QtWidgets.QWidget()
        device_host.setObjectName("transparentHost")
        device_grid = QtWidgets.QGridLayout(device_host)
        device_grid.setContentsMargins(12, 0, 8, 0)
        device_grid.setHorizontalSpacing(10)
        device_grid.setVerticalSpacing(8)
        device_grid.setColumnStretch(1, 1)

        row = 0
        device_grid.addWidget(_label("Device"), row, 0)
        self.device_mode_combo = QtWidgets.QComboBox()
        self.device_mode_combo.addItem("Default", DeviceOverrideMode.DEFAULT.value)
        self.device_mode_combo.addItem("GPU All", DeviceOverrideMode.ALL_GPUS.value)
        self.device_mode_combo.addItem("CPU", DeviceOverrideMode.CPU.value)
        self.device_mode_combo.addItem("Custom", DeviceOverrideMode.SPECIFIC_GPUS.value)
        self.device_mode_combo.currentIndexChanged.connect(self._emit_device_mode_changed)
        device_grid.addWidget(self.device_mode_combo, row, 1)
        row += 1

        self.device_selection_label = _label("Custom Devices")
        device_grid.addWidget(self.device_selection_label, row, 0, alignment=QtCore.Qt.AlignmentFlag.AlignTop)
        self.device_selection_container = QtWidgets.QWidget()
        self.device_selection_container.setObjectName("transparentHost")
        self.device_selection_layout = QtWidgets.QVBoxLayout(self.device_selection_container)
        self.device_selection_layout.setContentsMargins(0, 0, 0, 0)
        self.device_selection_layout.setSpacing(6)
        self.device_selection_empty = QtWidgets.QLabel("No render devices detected")
        self.device_selection_empty.setWordWrap(True)
        self.device_selection_layout.addWidget(self.device_selection_empty)
        device_grid.addWidget(self.device_selection_container, row, 1)
        row += 1

        self.device_section = CollapsibleSection("Device", device_host, self)
        self.device_section.setExpanded(True)
        layout.addWidget(self.device_section)
        layout.addStretch(1)

    @staticmethod
    def _set_combo_mixed_state(combo: QtWidgets.QComboBox, *, mixed: bool, value: str, fallback_text: str) -> None:
        mixed_data = "__mixed__"
        mixed_index = combo.findData(mixed_data)
        if mixed:
            if mixed_index < 0:
                combo.insertItem(0, "Mixed", mixed_data)
                mixed_index = 0
            combo.setCurrentIndex(mixed_index)
            return
        if mixed_index >= 0:
            combo.removeItem(mixed_index)
        target_index = combo.findData(value)
        combo.setCurrentIndex(target_index if target_index >= 0 else combo.findText(fallback_text))

    def _emit_device_mode_changed(self) -> None:
        if self._syncing:
            return
        self.device_mode_changed.emit(str(self.device_mode_combo.currentData() or DeviceOverrideMode.DEFAULT.value))

    def _emit_device_selection_changed(self) -> None:
        if self._syncing:
            return
        selected_ids = [device_id for device_id, checkbox in self._device_option_checks if checkbox.checkState() == QtCore.Qt.CheckState.Checked]
        self.device_selection_changed.emit(",".join(selected_ids))

    def _emit_retain_usd_clicked(self, checked: bool) -> None:
        if self._syncing:
            return
        self.retain_built_usd_changed.emit(bool(checked))

    def _emit_single_process_render_clicked(self, checked: bool) -> None:
        if self._syncing:
            return
        self.render_all_frames_single_process_changed.emit(bool(checked))

    def _emit_reuse_usd_clicked(self, checked: bool) -> None:
        if self._syncing:
            return
        self.reuse_retained_usd_changed.emit(bool(checked))

    def _emit_usd_output_directory_mode_changed(self) -> None:
        if self._syncing:
            return
        self.usd_output_directory_mode_changed.emit(str(self.usd_output_mode_combo.currentData() or UsdOutputDirectoryMode.DEFAULT_TEMP.value))

    def _emit_usd_output_directory_custom_path_changed(self) -> None:
        if self._syncing:
            return
        self.usd_output_directory_custom_path_changed.emit(self.usd_output_custom_path_value.text().strip())

    def _browse_usd_output_custom_path(self) -> None:
        start_dir = self.usd_output_custom_path_value.text().strip() or str(Path.home())
        path = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Select USD Output Folder",
            start_dir,
        )
        if path:
            self.usd_output_custom_path_value.setText(path)
            self._emit_usd_output_directory_custom_path_changed()

    def _refresh_usd_output_ui(self, *, editable: bool, mixed_mode: bool, single_process_enabled: bool) -> None:
        mode = UsdOutputDirectoryMode.coerce(self.usd_output_mode_combo.currentData())
        show_custom_path = (not mixed_mode) and mode is UsdOutputDirectoryMode.CUSTOM_PATH
        controls_enabled = editable and single_process_enabled
        self.usd_output_custom_path_label.setVisible(show_custom_path)
        self.usd_output_custom_path_host.setVisible(show_custom_path)
        self.usd_output_custom_path_value.setEnabled(controls_enabled and show_custom_path)
        self.btn_browse_usd_output_custom_path.setEnabled(controls_enabled and show_custom_path)

    @staticmethod
    def _check_state_value(state: Any) -> int:
        return int(getattr(state, "value", state))

    def _rebuild_device_option_checks(self, device_options: list[dict[str, Any]], *, editable: bool) -> None:
        while self.device_selection_layout.count():
            item = self.device_selection_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._device_option_checks = []
        if not device_options:
            self.device_selection_empty = QtWidgets.QLabel("No render devices detected")
            self.device_selection_empty.setWordWrap(True)
            self.device_selection_layout.addWidget(self.device_selection_empty)
            return
        for option in device_options:
            checkbox = QtWidgets.QCheckBox(str(option.get("name", "") or "Unnamed Device"))
            check_state = QtCore.Qt.CheckState(self._check_state_value(option.get("check_state", QtCore.Qt.CheckState.Unchecked)))
            checkbox.setTristate(check_state == QtCore.Qt.CheckState.PartiallyChecked)
            checkbox.setCheckState(check_state)
            checkbox.setEnabled(editable and bool(option.get("enabled", True)))
            checkbox.clicked.connect(self._emit_device_selection_changed)
            self.device_selection_layout.addWidget(checkbox)
            self._device_option_checks.append((str(option.get("id", "") or ""), checkbox))
        self.device_selection_layout.addStretch(1)

    def set_state(self, state: dict[str, Any]) -> None:
        self._syncing = True
        try:
            self.name_value.setText(str(state.get("name_text", "-") or "-"))
            self.file_value.setText(str(state.get("file_text", "-") or "-"))
            self.rop_value.setText(str(state.get("rop_text", "-") or "-"))

            editable = bool(state.get("editable", False))
            mixed_device_mode = bool(state.get("mixed_device_mode", False))
            device_mode = DeviceOverrideMode.coerce(state.get("device_mode"))
            self._set_combo_mixed_state(
                self.device_mode_combo,
                mixed=mixed_device_mode,
                value=device_mode.value,
                fallback_text="Default",
            )
            self.device_mode_combo.setEnabled(editable)
            show_custom_devices = bool(state.get("show_custom_devices", False))
            self.device_selection_label.setVisible(show_custom_devices)
            self.device_selection_container.setVisible(show_custom_devices)
            self._rebuild_device_option_checks(
                list(state.get("device_options", []) or []),
                editable=editable and bool(state.get("device_selection_enabled", False)),
            )

            single_process_state = self._check_state_value(
                state.get("single_process_render_check_state", self._check_state_value(QtCore.Qt.CheckState.Unchecked))
            )
            self.single_process_render_check.setTristate(
                single_process_state == self._check_state_value(QtCore.Qt.CheckState.PartiallyChecked)
            )
            self.single_process_render_check.setCheckState(QtCore.Qt.CheckState(single_process_state))
            self.single_process_render_check.setEnabled(editable)
            single_process_active = single_process_state == self._check_state_value(QtCore.Qt.CheckState.Checked)

            retain_state = self._check_state_value(
                state.get("retain_check_state", self._check_state_value(QtCore.Qt.CheckState.Unchecked))
            )
            self.retain_usd_check.setTristate(retain_state == self._check_state_value(QtCore.Qt.CheckState.PartiallyChecked))
            self.retain_usd_check.setCheckState(QtCore.Qt.CheckState(retain_state))
            self.retain_usd_check.setEnabled(editable and single_process_active)
            reuse_state = self._check_state_value(
                state.get("reuse_check_state", self._check_state_value(QtCore.Qt.CheckState.Unchecked))
            )
            self.reuse_usd_check.setTristate(reuse_state == self._check_state_value(QtCore.Qt.CheckState.PartiallyChecked))
            self.reuse_usd_check.setCheckState(QtCore.Qt.CheckState(reuse_state))
            self.reuse_usd_check.setEnabled(editable and single_process_active and bool(state.get("reuse_enabled", True)))
            mixed_usd_output_mode = bool(state.get("mixed_usd_output_mode", False))
            usd_output_mode = UsdOutputDirectoryMode.coerce(state.get("usd_output_directory_mode"))
            self._set_combo_mixed_state(
                self.usd_output_mode_combo,
                mixed=mixed_usd_output_mode,
                value=usd_output_mode.value,
                fallback_text=UsdOutputDirectoryMode.DEFAULT_TEMP.label(),
            )
            self.usd_output_mode_combo.setEnabled(editable and single_process_active)
            mixed_usd_output_custom_path = bool(state.get("mixed_usd_output_custom_path", False))
            self.usd_output_custom_path_value.setText("" if mixed_usd_output_custom_path else str(state.get("usd_output_directory_custom_path", "") or ""))
            self.usd_output_custom_path_value.setPlaceholderText("Mixed" if mixed_usd_output_custom_path else "Folder path")
            self._refresh_usd_output_ui(
                editable=editable,
                mixed_mode=mixed_usd_output_mode,
                single_process_enabled=single_process_active,
            )

            self.retained_usd_value.setText(str(state.get("retained_usd_path", "") or ""))
            self.retained_usd_built_range.setText(str(state.get("retained_usd_built_range", "-") or "-"))
            self.retained_usd_built_step.setText(str(state.get("retained_usd_built_step", "-") or "-"))
            self.retained_usd_built_at.setText(str(state.get("retained_usd_built_at", "-") or "-"))
            self.retained_usd_status.setText(str(state.get("retained_usd_status", "-") or "-"))
            warning_text = str(state.get("retained_usd_warning", "") or "")
            self.retained_usd_warning.setText(warning_text)
            self.retained_usd_warning.setVisible(bool(warning_text))
            self.retained_usd_warning_label.setVisible(bool(warning_text))
            self.btn_reveal_retained_usd.setEnabled(bool(state.get("can_open", state.get("can_reveal", False))))
            self.btn_delete_retained_usd.setEnabled(bool(state.get("can_delete", False)))
        finally:
            self._syncing = False


class RopListWidget(QtWidgets.QListWidget):
    """ROP list with persistent row striping, including empty area."""

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        super().paintEvent(event)
        vp = self.viewport()
        if vp is None:
            return

        painter = QtGui.QPainter(vp)
        try:
            rect = vp.rect()
            if rect.height() <= 0 or rect.width() <= 0:
                return

            base = QtGui.QColor("#353535")
            alt = QtGui.QColor("#383838")
            hinted = self.sizeHintForRow(0)
            row_h = int(hinted) if int(hinted) > 0 else max(24, self.fontMetrics().height() + 14)

            start_y = 0
            if self.count() > 0:
                last = self.item(self.count() - 1)
                last_rect = self.visualItemRect(last)
                start_y = max(0, last_rect.bottom() + 1)
                row_index = self.count()
            else:
                row_index = 0

            y = start_y
            while y < rect.height():
                stripe = alt if (row_index % 2) else base
                painter.fillRect(QtCore.QRect(0, y, rect.width(), min(row_h, rect.height() - y)), stripe)
                y += row_h
                row_index += 1
        finally:
            painter.end()


class AddJobPanel(QtWidgets.QGroupBox):
    add_job_requested = QtCore.Signal(dict)
    scan_requested = QtCore.Signal(dict)

    def __init__(self, config: Any, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__("Create Job", parent)
        self.config = config
        self._rop_scan_meta: dict[str, dict[str, Any]] = {}
        self._ui_running = False
        self._ui_scan_in_progress = False
        self._build_ui()
        self._load_recents()

    def _build_ui(self) -> None:
        layout = QtWidgets.QGridLayout(self)
        layout.setColumnStretch(1, 1)
        layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop | QtCore.Qt.AlignmentFlag.AlignLeft)

        row = 0
        layout.addWidget(QtWidgets.QLabel("HIP File"), row, 0)
        row += 1
        self.hip_combo = QtWidgets.QComboBox()
        self.hip_combo.setEditable(True)
        self.hip_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        layout.addWidget(self.hip_combo, row, 0, 1, 3)
        row += 1

        hip_actions = QtWidgets.QHBoxLayout()
        hip_actions.setContentsMargins(0, 0, 0, 0)
        self.btn_browse_hip = QtWidgets.QPushButton("Browse...")
        self.btn_browse_hip.clicked.connect(self._browse_hip)
        hip_actions.addWidget(self.btn_browse_hip)
        self.btn_scan_out = QtWidgets.QPushButton("Reload")
        self.btn_scan_out.clicked.connect(self._request_scan)
        hip_actions.addWidget(self.btn_scan_out)
        hip_actions.addStretch(1)
        hip_actions_host = QtWidgets.QWidget()
        hip_actions_host.setObjectName("transparentHost")
        hip_actions_host.setLayout(hip_actions)
        layout.addWidget(hip_actions_host, row, 0, 1, 3)
        row += 1

        self.rop_panel = QtWidgets.QFrame()
        self.rop_panel.setObjectName("ropPanel")
        rop_panel_layout = QtWidgets.QVBoxLayout(self.rop_panel)
        rop_panel_layout.setContentsMargins(0, 0, 0, 0)
        rop_panel_layout.setSpacing(0)

        rop_header = QtWidgets.QWidget()
        rop_header.setObjectName("ropPanelHeader")
        rop_header_layout = QtWidgets.QHBoxLayout(rop_header)
        rop_header_layout.setContentsMargins(10, 6, 10, 6)
        rop_header_layout.setSpacing(8)
        rop_title = QtWidgets.QLabel("ROPs")
        rop_title.setObjectName("ropPanelTitle")
        rop_header_layout.addWidget(rop_title)

        self.chk_scan_out = QtWidgets.QCheckBox("/out")
        self.chk_scan_stage = QtWidgets.QCheckBox("/stage")
        self.chk_scan_out.setChecked(True)
        self.chk_scan_stage.setChecked(True)
        rop_header_layout.addWidget(self.chk_scan_out)
        rop_header_layout.addWidget(self.chk_scan_stage)
        rop_header_layout.addStretch(1)
        rop_panel_layout.addWidget(rop_header)

        self.rop_list = RopListWidget()
        self.rop_list.setObjectName("ropList")
        self.rop_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.rop_list.itemSelectionChanged.connect(self._refresh_override_warning)
        self.rop_list.setMinimumHeight(160)
        self.rop_list.setAlternatingRowColors(True)
        rop_panel_layout.addWidget(self.rop_list, 1)

        layout.addWidget(self.rop_panel, row, 0, 1, 3)
        row += 1

        layout.addWidget(QtWidgets.QLabel("Name (Optional)"), row, 0)
        self.name_edit = QtWidgets.QLineEdit()
        self.name_edit.setPlaceholderText("Defaults to hip filename + rop node")
        layout.addWidget(self.name_edit, row, 1, 1, 2)
        row += 1

        self.frames_group = QtWidgets.QGroupBox("Frames")
        frames_layout = QtWidgets.QGridLayout(self.frames_group)
        frames_layout.setColumnStretch(1, 1)

        frame_mode_row = QtWidgets.QHBoxLayout()
        frame_mode_row.setContentsMargins(0, 0, 0, 0)
        self.radio_use_rop = QtWidgets.QRadioButton("Use from ROP")
        self.radio_override = QtWidgets.QRadioButton("Override")
        self.radio_use_rop.setChecked(True)
        self.radio_use_rop.toggled.connect(self._update_frame_controls_enabled)
        self.radio_override.toggled.connect(self._refresh_override_warning)
        frame_mode_row.addWidget(self.radio_use_rop)
        frame_mode_row.addWidget(self.radio_override)
        frame_mode_row.addStretch(1)
        frame_mode_host = QtWidgets.QWidget()
        frame_mode_host.setLayout(frame_mode_row)
        frames_layout.addWidget(frame_mode_host, 0, 0, 1, 2)

        self.override_warning_frame = QtWidgets.QFrame()
        self.override_warning_frame.setObjectName("overrideStrictWarning")
        self.override_warning_frame.setStyleSheet(
            "#overrideStrictWarning { border: none; border-radius: 8px; background: #6b3f00; }"
        )
        warning_layout = QtWidgets.QHBoxLayout(self.override_warning_frame)
        warning_layout.setContentsMargins(12, 8, 12, 8)
        self.override_warning_label = QtWidgets.QLabel("Cannot override! ROP set to \"Strict\" frame range.")
        self.override_warning_label.setStyleSheet("background: transparent;")
        self.override_warning_label.setWordWrap(True)
        warning_layout.addWidget(self.override_warning_label)
        self.override_warning_frame.setVisible(False)
        frames_layout.addWidget(self.override_warning_frame, 1, 0, 1, 2)
        row += 1

        self.start_spin = CleanStepSpinBox()
        self.end_spin = CleanStepSpinBox()
        self.step_spin = CleanStepSpinBox()
        for spin in (self.start_spin, self.end_spin):
            spin.setRange(-1000000, 10000000)
        self.step_spin.setRange(1, 1000000)
        self.start_spin.setValue(1)
        self.end_spin.setValue(1)
        self.step_spin.setValue(1)
        frames_layout.addWidget(QtWidgets.QLabel("Start"), 2, 0)
        frames_layout.addWidget(self.start_spin, 2, 1)
        frames_layout.addWidget(QtWidgets.QLabel("End"), 3, 0)
        frames_layout.addWidget(self.end_spin, 3, 1)
        frames_layout.addWidget(QtWidgets.QLabel("Step"), 4, 0)
        frames_layout.addWidget(self.step_spin, 4, 1)

        layout.addWidget(self.frames_group, row, 0, 1, 3)
        self.frames_group.setVisible(False)
        row += 1

        self.btn_add_queue = QtWidgets.QPushButton("Add To Queue")
        self.btn_add_queue.clicked.connect(self._emit_add_job)
        layout.addWidget(self.btn_add_queue, row, 0, 1, 3)
        row += 1
        layout.setRowStretch(row, 1)

        self._update_frame_controls_enabled()

    def _load_recents(self) -> None:
        for hip in self.config.get("recent_hip_paths", []):
            self.hip_combo.addItem(str(hip))

    def _browse_hip(self) -> None:
        start_dir = self.config.get("last_hip_dir", "") or str(Path.home())
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Select Houdini HIP File",
            start_dir,
            "Houdini Files (*.hip *.hiplc *.hipnc);;All Files (*.*)",
        )
        if not path:
            return
        self.load_hip_path(path, request_scan=True)

    def load_hip_path(self, path: str, *, request_scan: bool = True) -> None:
        text = str(path or "").strip()
        if not text:
            return
        self.hip_combo.setEditText(text)
        self.config.set("last_hip_dir", str(Path(text).parent))
        if request_scan and not self._ui_scan_in_progress:
            self._request_scan(text)

    def _request_scan(self, hip_path_override: str | None = None) -> None:
        self.scan_requested.emit(
            {
                "hip_path": str(hip_path_override or self.hip_combo.currentText()).strip(),
                "scan_out": self.chk_scan_out.isChecked(),
                "scan_stage": self.chk_scan_stage.isChecked(),
            }
        )

    def _update_frame_controls_enabled(self) -> None:
        enabled = self.radio_override.isChecked() and not self._is_override_strict_conflict()
        for w in (self.start_spin, self.end_spin, self.step_spin):
            w.setEnabled(enabled)

    def _emit_add_job(self) -> None:
        selected_rop_paths = self.selected_rop_paths()
        if not selected_rop_paths:
            return
        payload = {
            "hip_path": self.hip_combo.currentText().strip(),
            "rop_path": selected_rop_paths[0],
            "rop_paths": selected_rop_paths,
            "name": self.name_edit.text().strip(),
            "frame_range_mode": "use_rop",
            "start_frame": None,
            "end_frame": None,
            "step": None,
        }
        self.add_job_requested.emit(payload)

    def set_enabled_for_run_state(self, running: bool, scan_in_progress: bool) -> None:
        self._ui_running = bool(running)
        self._ui_scan_in_progress = bool(scan_in_progress)
        self.btn_add_queue.setEnabled(self._can_add_job_now())
        self.btn_browse_hip.setEnabled(not scan_in_progress)
        self.btn_scan_out.setEnabled(not scan_in_progress)
        self.hip_combo.setEnabled(not scan_in_progress)
        self.chk_scan_out.setEnabled(not scan_in_progress)
        self.chk_scan_stage.setEnabled(not scan_in_progress)
        self.rop_list.setEnabled(not scan_in_progress)
        self.name_edit.setEnabled(not scan_in_progress)
        self.radio_use_rop.setEnabled(not scan_in_progress)
        self.radio_override.setEnabled(not scan_in_progress)
        self._update_frame_controls_enabled()
        self._refresh_override_warning()
        if scan_in_progress:
            for w in (self.start_spin, self.end_spin, self.step_spin):
                w.setEnabled(False)

    def current_rop_path(self) -> str:
        paths = self.selected_rop_paths()
        return paths[0] if paths else ""

    def selected_rop_paths(self) -> list[str]:
        result: list[str] = []
        for item in self.rop_list.selectedItems():
            path = str(item.data(QtCore.Qt.ItemDataRole.UserRole) or item.text()).strip()
            if path:
                result.append(path)
        return result

    def rop_strict_frame_range_for_path(self, rop_path: str) -> bool:
        rec = self._rop_scan_meta.get((rop_path or "").strip())
        return bool(rec and rec.get("strict_frame_range"))

    def rop_output_path_for_path(self, rop_path: str) -> str:
        rec = self._rop_scan_meta.get((rop_path or "").strip())
        return str((rec or {}).get("output_path", "") or "")

    def rop_range_info_for_path(self, rop_path: str) -> tuple[float | None, float | None, float | None]:
        rec = self._rop_scan_meta.get((rop_path or "").strip()) or {}
        return (
            rec.get("runtime_start_frame"),
            rec.get("runtime_end_frame"),
            rec.get("runtime_step"),
        )

    def rop_all_frames_single_process_for_path(self, rop_path: str) -> bool:
        rec = self._rop_scan_meta.get((rop_path or "").strip())
        return bool(rec and rec.get("all_frames_single_process"))

    def set_scanned_rops(self, rop_records: list[dict[str, Any]]) -> None:
        selected_before = set(self.selected_rop_paths())
        current_before = self.current_rop_path()
        scroll_before = self.rop_list.verticalScrollBar().value() if self.rop_list.verticalScrollBar() is not None else 0
        for rec in rop_records:
            path_key = str(rec.get("path", "")).strip()
            if path_key:
                self._rop_scan_meta[path_key] = dict(rec)
        self.rop_list.blockSignals(True)
        self.rop_list.clear()
        first_item: QtWidgets.QListWidgetItem | None = None
        for rec in rop_records:
            path = str(rec.get("path", "")).strip()
            if not path:
                continue
            item = QtWidgets.QListWidgetItem(path)
            item.setData(QtCore.Qt.ItemDataRole.UserRole, path)
            self.rop_list.addItem(item)
            if path in selected_before:
                item.setSelected(True)
            if current_before and path == current_before:
                self.rop_list.setCurrentItem(item)
            if first_item is None:
                first_item = item
        if self.rop_list.selectedItems() == [] and first_item is not None:
            first_item.setSelected(True)
            if self.rop_list.currentItem() is None:
                self.rop_list.setCurrentItem(first_item)
        self.rop_list.blockSignals(False)
        if self.rop_list.verticalScrollBar() is not None:
            self.rop_list.verticalScrollBar().setValue(scroll_before)
        current_item = self.rop_list.currentItem()
        if current_item is not None:
            self.rop_list.scrollToItem(current_item, QtWidgets.QAbstractItemView.ScrollHint.PositionAtCenter)
        self._refresh_override_warning()

    def _current_rop_scan_record(self) -> dict[str, Any] | None:
        paths = self.selected_rop_paths()
        if not paths:
            return None
        return self._rop_scan_meta.get(paths[0])

    def current_rop_is_strict_frame_range(self) -> bool:
        return any(self.rop_strict_frame_range_for_path(p) for p in self.selected_rop_paths())

    def set_rop_strict_frame_range_hint(self, rop_path: str, is_strict: bool) -> None:
        rop_path = (rop_path or "").strip()
        if not rop_path:
            return
        rec = dict(self._rop_scan_meta.get(rop_path, {}))
        rec["path"] = rop_path
        rec["strict_frame_range"] = bool(is_strict)
        self._rop_scan_meta[rop_path] = rec
        if rop_path in self.selected_rop_paths():
            self._refresh_override_warning()

    def set_rop_output_path_hint(self, rop_path: str, output_path: str) -> None:
        rop_path = (rop_path or "").strip()
        if not rop_path:
            return
        rec = dict(self._rop_scan_meta.get(rop_path, {}))
        rec["path"] = rop_path
        rec["output_path"] = str(output_path or "")
        self._rop_scan_meta[rop_path] = rec

    def set_rop_range_hint(
        self,
        rop_path: str,
        start_frame: float | None,
        end_frame: float | None,
        step: float | None,
    ) -> None:
        rop_path = (rop_path or "").strip()
        if not rop_path:
            return
        rec = dict(self._rop_scan_meta.get(rop_path, {}))
        rec["path"] = rop_path
        rec["runtime_start_frame"] = start_frame
        rec["runtime_end_frame"] = end_frame
        rec["runtime_step"] = step
        self._rop_scan_meta[rop_path] = rec

    def set_rop_all_frames_single_process_hint(self, rop_path: str, enabled: bool) -> None:
        rop_path = (rop_path or "").strip()
        if not rop_path:
            return
        rec = dict(self._rop_scan_meta.get(rop_path, {}))
        rec["path"] = rop_path
        rec["all_frames_single_process"] = bool(enabled)
        self._rop_scan_meta[rop_path] = rec

    def _refresh_override_warning(self, *_args: object) -> None:
        show = self._is_override_strict_conflict()
        self.override_warning_frame.setVisible(show)
        self._update_frame_controls_enabled()
        self.btn_add_queue.setEnabled(self._can_add_job_now())

    def _is_override_strict_conflict(self) -> bool:
        return self.radio_override.isChecked() and self.current_rop_is_strict_frame_range()

    def _can_add_job_now(self) -> bool:
        if self._ui_scan_in_progress:
            return False
        if not self.selected_rop_paths():
            return False
        return True

    def push_recents_from_job(self, hip_path: str, rop_path: str) -> None:
        self.config.push_recent("recent_hip_paths", hip_path)
        self.config.push_recent("recent_rop_paths", rop_path)
        self._refresh_combo_from_config(self.hip_combo, self.config.get("recent_hip_paths", []))

    @staticmethod
    def _refresh_combo_from_config(combo: QtWidgets.QComboBox, items: list[str]) -> None:
        current = combo.currentText()
        combo.blockSignals(True)
        combo.clear()
        combo.addItems([str(i) for i in items])
        combo.setEditText(current)
        combo.blockSignals(False)


class QueueTableView(QtWidgets.QTableView):
    FRAME_HANDLING_COLUMN = 5
    FRAME_HANDLING_OPTIONS = [
        "Render Missing",
        "Render From First Missing",
        "Overwrite",
    ]
    AUTOSCROLL_INTERVAL_MS = 16
    AUTOSCROLL_DEADZONE_PX = 10
    AUTOSCROLL_MAX_STEP = 48

    row_reordered_by_drag = QtCore.Signal(int, int)
    rows_reordered_by_drag = QtCore.Signal(list, int)
    frame_handling_chosen = QtCore.Signal(int, str)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.selection_row_color = QtGui.QColor(DEFAULT_THEME["selection_row"])
        self.selection_row_alt_color = QtGui.QColor(DEFAULT_THEME["selection_row_alt"])
        self.selection_overlay_opacity = int(DEFAULT_THEME.get("selection_overlay_opacity", 95))
        self.path_sync_overlay_opacity = int(DEFAULT_THEME.get("path_sync_overlay_opacity", 28))
        self.stats_split_after_visual_index: int | None = None
        self.stats_split_line_color = QtGui.QColor("#5a5a5a")
        self.progress_usd_build_color = QtGui.QColor(DEFAULT_THEME.get("progress_usd_build", "#11b7b7"))
        self.progress_render_color = QtGui.QColor(DEFAULT_THEME.get("progress_render", "#2fbf4a"))
        self.combo_bg_color = QtGui.QColor(DEFAULT_THEME.get("button_bg", "#3a3a3a"))
        self.combo_text_color = QtGui.QColor(DEFAULT_THEME.get("button_text", "#ffffff"))
        self.combo_border_color = QtGui.QColor("#555555")
        self._suppress_drag_select_until_release = False
        self._pending_preserved_drag_rows: list[int] = []
        self._pending_preserved_drag_pos: QtCore.QPoint | None = None
        self._drop_indicator_row: int | None = None
        self._inline_editor: SafeCommitLineEdit | None = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        self._inline_edit_selected_rows: list[int] = []
        self._frame_handling_target_rows: list[int] = []
        self._suppress_frame_handling_popup_once = False
        self._suppress_enter_retrigger_once = False
        self._autoscroll_active = False
        self._autoscroll_anchor_global: QtCore.QPoint | None = None
        self._autoscroll_anchor_viewport: QtCore.QPoint | None = None
        self._autoscroll_timer = QtCore.QTimer(self)
        self._autoscroll_timer.setInterval(self.AUTOSCROLL_INTERVAL_MS)
        self._autoscroll_timer.timeout.connect(self._tick_autoscroll)

    @classmethod
    def _autoscroll_step_for_offset(cls, offset: int) -> int:
        magnitude = abs(int(offset))
        if magnitude <= cls.AUTOSCROLL_DEADZONE_PX:
            return 0
        effective = magnitude - cls.AUTOSCROLL_DEADZONE_PX
        step = min(cls.AUTOSCROLL_MAX_STEP, max(1, int(round((effective ** 1.1) / 5.0))))
        return -step if offset < 0 else step

    def _start_autoscroll(self, event: QtGui.QMouseEvent) -> None:
        self._autoscroll_active = True
        self._autoscroll_anchor_global = event.globalPosition().toPoint()
        self._autoscroll_anchor_viewport = event.position().toPoint()
        self.viewport().setCursor(QtCore.Qt.CursorShape.SizeAllCursor)
        self._autoscroll_timer.start()
        self.viewport().update()

    def _stop_autoscroll(self) -> None:
        if not self._autoscroll_active and not self._autoscroll_timer.isActive():
            return
        self._autoscroll_active = False
        self._autoscroll_anchor_global = None
        self._autoscroll_anchor_viewport = None
        self._autoscroll_timer.stop()
        self.viewport().unsetCursor()
        self.viewport().update()

    def _autoscroll_should_stop(self, cursor_pos: QtCore.QPoint) -> bool:
        if not self._autoscroll_active or self._autoscroll_anchor_global is None:
            return True
        if not bool(QtWidgets.QApplication.mouseButtons() & QtCore.Qt.MouseButton.MiddleButton):
            return True
        window = self.window()
        if isinstance(window, QtWidgets.QWidget) and not window.frameGeometry().contains(cursor_pos):
            return True
        return False

    def _paint_autoscroll_marker(self, painter: QtGui.QPainter) -> None:
        if not self._autoscroll_active or self._autoscroll_anchor_viewport is None:
            return
        center = self._autoscroll_anchor_viewport
        marker_rect = QtCore.QRect(center.x() - 8, center.y() - 8, 16, 16)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        painter.setPen(QtGui.QPen(QtGui.QColor(190, 190, 190, 220), 1))
        painter.setBrush(QtGui.QColor(28, 28, 28, 200))
        painter.drawEllipse(marker_rect)
        painter.drawLine(center.x() - 4, center.y(), center.x() + 4, center.y())
        painter.drawLine(center.x(), center.y() - 4, center.x(), center.y() + 4)

    def _tick_autoscroll(self) -> None:
        cursor_pos = QtGui.QCursor.pos()
        if self._autoscroll_should_stop(cursor_pos):
            self._stop_autoscroll()
            return
        offset = cursor_pos - self._autoscroll_anchor_global
        dx = self._autoscroll_step_for_offset(offset.x())
        dy = self._autoscroll_step_for_offset(offset.y())
        if dx:
            hbar = self.horizontalScrollBar()
            hbar.setValue(hbar.value() + dx)
        if dy:
            vbar = self.verticalScrollBar()
            vbar.setValue(vbar.value() + dy)

    def clear_frame_handling_interaction_state(self) -> None:
        self._suppress_drag_select_until_release = False

    def _clear_drop_indicator(self) -> None:
        if self._drop_indicator_row is None:
            return
        self._drop_indicator_row = None
        self.viewport().update()

    def _drop_target_row_from_y(self, y: int) -> int:
        if self.rowCount() <= 0:
            return 0
        row = self.rowAt(y)
        if row < 0:
            last = self.rowCount() - 1
            last_rect = self._row_visual_rect(last)
            if not last_rect.isValid():
                return self.rowCount()
            return self.rowCount() if y > last_rect.bottom() else 0
        rect = self._row_visual_rect(row)
        if rect.isValid() and y > rect.center().y():
            return row + 1
        return row

    def _set_drop_indicator_from_pos(self, pos: QtCore.QPoint) -> None:
        new_row = self._drop_target_row_from_y(pos.y())
        if new_row != self._drop_indicator_row:
            self._drop_indicator_row = new_row
            self.viewport().update()

    def _viewport_drag_pos(self) -> QtCore.QPoint:
        return self.viewport().mapFromGlobal(QtGui.QCursor.pos())

    def _first_visible_column(self) -> int:
        for col in range(self.columnCount()):
            if not self.isColumnHidden(col):
                return col
        return 0

    def _row_visual_rect(self, row: int) -> QtCore.QRect:
        if not (0 <= row < self.rowCount()):
            return QtCore.QRect()
        col = self._first_visible_column()
        return self.visualRect(self.model().index(row, col))

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        super().paintEvent(event)
        # Draw the stats split divider at all times.
        painter = QtGui.QPainter(self.viewport())
        try:
            if self.stats_split_after_visual_index is not None and self.horizontalHeader() is not None:
                visual_idx = int(self.stats_split_after_visual_index)
                header = self.horizontalHeader()
                if 0 <= visual_idx < self.columnCount():
                    logical = header.logicalIndex(visual_idx)
                    if 0 <= logical < self.columnCount() and not self.isColumnHidden(logical):
                        x = self.columnViewportPosition(logical) + self.columnWidth(logical)
                        if 0 <= x <= self.viewport().width():
                            split_pen = QtGui.QPen(self.stats_split_line_color)
                            split_pen.setWidth(1)
                            painter.setPen(split_pen)
                            painter.drawLine(x, 0, x, self.viewport().height())
                if self._drop_indicator_row is not None:
                    row = max(0, min(self._drop_indicator_row, self.rowCount()))
                    if row >= self.rowCount():
                        if self.rowCount() > 0:
                            last_rect = self._row_visual_rect(self.rowCount() - 1)
                            y = last_rect.bottom() + 1
                        else:
                            y = 0
                    else:
                        row_rect = self._row_visual_rect(row)
                        y = row_rect.top()
                    pen = QtGui.QPen(self.selection_row_color)
                    pen.setWidth(2)
                    painter.setPen(pen)
                    painter.drawLine(0, y, self.viewport().width(), y)
            self._paint_autoscroll_marker(painter)
        finally:
            painter.end()

    def _selection_model(self) -> QtCore.QItemSelectionModel | None:
        return self.selectionModel()

    def _selected_row_count(self) -> int:
        sm = self._selection_model()
        return len(sm.selectedRows()) if sm is not None else 0

    def currentRow(self) -> int:
        idx = self.currentIndex()
        return idx.row() if idx.isValid() else -1

    def rowCount(self) -> int:
        model = self.model()
        return int(model.rowCount()) if model is not None else 0

    def columnCount(self) -> int:
        model = self.model()
        return int(model.columnCount()) if model is not None else 0

    def setCurrentCell(self, row: int, column: int) -> None:
        model = self.model()
        if model is None or row < 0 or column < 0:
            self.setCurrentIndex(QtCore.QModelIndex())
            return
        self.setCurrentIndex(model.index(row, column))

    def _supports_inline_text_edit(self, index: QtCore.QModelIndex) -> bool:
        if not index.isValid() or index.column() == self.FRAME_HANDLING_COLUMN:
            return False
        model = self.model()
        return bool(model is not None and (model.flags(index) & QtCore.Qt.ItemFlag.ItemIsEditable))

    def _inline_editor_rect(self, index: QtCore.QModelIndex) -> QtCore.QRect:
        rect = self.visualRect(index)
        return rect.adjusted(1, 1, -1, -1)

    def _update_inline_editor_geometry(self) -> None:
        if self._inline_editor is None or not self._inline_edit_index.isValid():
            return
        rect = self._inline_editor_rect(QtCore.QModelIndex(self._inline_edit_index))
        if not rect.isValid() or rect.width() <= 0 or rect.height() <= 0:
            self._cancel_inline_edit()
            return
        self._inline_editor.setGeometry(rect)

    def _start_inline_edit(self, index: QtCore.QModelIndex) -> bool:
        if not self._supports_inline_text_edit(index):
            return False
        if self._inline_edit_index.isValid() and QtCore.QModelIndex(self._inline_edit_index) == index and self._inline_editor is not None:
            self._inline_editor.setFocus()
            self._inline_editor.selectAll()
            return True
        self._cancel_inline_edit()
        sm = self._selection_model()
        rows = sorted({idx.row() for idx in sm.selectedRows() if idx.isValid()}) if sm is not None else []
        self._inline_edit_selected_rows = rows or [index.row()]
        self._inline_edit_index = QtCore.QPersistentModelIndex(index)
        editor = SafeCommitLineEdit(self.viewport())
        editor.setText(str(index.data(QtCore.Qt.ItemDataRole.EditRole) or index.data(QtCore.Qt.ItemDataRole.DisplayRole) or ""))
        editor.commit_requested.connect(self._commit_inline_edit)
        editor.cancel_requested.connect(self._cancel_inline_edit)
        self._inline_editor = editor
        self._update_inline_editor_geometry()
        editor.show()
        editor.setFocus(QtCore.Qt.FocusReason.OtherFocusReason)
        editor.selectAll()
        return True

    @staticmethod
    def _should_start_inline_edit(
        trigger: QtWidgets.QAbstractItemView.EditTrigger,
        event: QtCore.QEvent | None,
    ) -> bool:
        if trigger in {
            QtWidgets.QAbstractItemView.EditTrigger.CurrentChanged,
            QtWidgets.QAbstractItemView.EditTrigger.SelectedClicked,
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers,
        }:
            return False
        if trigger in {
            QtWidgets.QAbstractItemView.EditTrigger.DoubleClicked,
            QtWidgets.QAbstractItemView.EditTrigger.EditKeyPressed,
            QtWidgets.QAbstractItemView.EditTrigger.AnyKeyPressed,
        }:
            return True
        if trigger == QtWidgets.QAbstractItemView.EditTrigger.AllEditTriggers:
            return isinstance(event, (QtGui.QMouseEvent, QtGui.QKeyEvent))
        return False

    def _commit_inline_edit(self, text: str) -> None:
        editor = self._inline_editor
        index = QtCore.QModelIndex(self._inline_edit_index)
        self._inline_editor = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        if isinstance(editor, SafeCommitLineEdit) and editor.committed_via_enter():
            self._suppress_enter_retrigger_once = True
        if QtWidgets.QApplication.mouseButtons() != QtCore.Qt.MouseButton.NoButton:
            self._suppress_frame_handling_popup_once = True
        if editor is not None:
            editor.hide()
            editor.deleteLater()
        model = self.model()
        if model is None or not index.isValid():
            self._inline_edit_selected_rows = []
            return
        model.setData(index, text, QtCore.Qt.ItemDataRole.EditRole)
        self._restore_inline_edit_selection(index.row())
        self._inline_edit_selected_rows = []

    def _cancel_inline_edit(self) -> None:
        editor = self._inline_editor
        self._inline_editor = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        self._inline_edit_selected_rows = []
        if editor is not None:
            editor.hide()
            editor.deleteLater()

    def inline_edit_target_rows(self) -> list[int]:
        return list(self._inline_edit_selected_rows)

    def consume_frame_handling_target_rows(self) -> list[int]:
        rows = list(self._frame_handling_target_rows)
        self._frame_handling_target_rows = []
        return rows

    def _clear_frame_handling_popup_suppression(self) -> None:
        self._suppress_frame_handling_popup_once = False

    def _restore_inline_edit_selection(self, current_row: int) -> None:
        rows = [row for row in self._inline_edit_selected_rows if 0 <= row < self.rowCount()]
        if not rows:
            return
        sm = self._selection_model()
        model = self.model()
        if sm is None or model is None:
            return
        blocker = QtCore.QSignalBlocker(sm)
        try:
            sm.clearSelection()
            for row in rows:
                idx = model.index(row, 0)
                sm.select(
                    idx,
                    QtCore.QItemSelectionModel.SelectionFlag.Select
                    | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                )
            current_target = current_row if current_row in rows else rows[0]
            sm.setCurrentIndex(model.index(current_target, 0), QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)
        finally:
            del blocker

    def edit(
        self,
        index: QtCore.QModelIndex,
        trigger: QtWidgets.QAbstractItemView.EditTrigger = QtWidgets.QAbstractItemView.EditTrigger.AllEditTriggers,
        event: QtCore.QEvent | None = None,
    ) -> bool:
        if (
            index.isValid()
            and index.column() == self.FRAME_HANDLING_COLUMN
            and self._should_start_inline_edit(trigger, event)
        ):
            self._show_frame_handling_popup(index)
            return True
        if self._should_start_inline_edit(trigger, event) and self._start_inline_edit(index):
            return True
        return False

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        if self._autoscroll_active and event.key() == QtCore.Qt.Key.Key_Escape:
            self._stop_autoscroll()
            event.accept()
            return
        if self._suppress_enter_retrigger_once and event.key() in {QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter}:
            self._suppress_enter_retrigger_once = False
            event.accept()
            return
        idx = self.currentIndex()
        if idx.isValid():
            if event.key() in {QtCore.Qt.Key.Key_F2, QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter}:
                if idx.column() == self.FRAME_HANDLING_COLUMN:
                    self._show_frame_handling_popup(idx)
                else:
                    self._start_inline_edit(idx)
                event.accept()
                return
            if (
                not event.text().isspace()
                and event.text()
                and event.modifiers() in {QtCore.Qt.KeyboardModifier.NoModifier, QtCore.Qt.KeyboardModifier.ShiftModifier}
                and self._supports_inline_text_edit(idx)
            ):
                if self._start_inline_edit(idx) and self._inline_editor is not None:
                    self._inline_editor.setText(event.text())
                    self._inline_editor.setCursorPosition(len(event.text()))
                    event.accept()
                    return
        super().keyPressEvent(event)

    def _is_row_selected(self, row: int) -> bool:
        sm = self._selection_model()
        return bool(sm is not None and sm.isRowSelected(row, QtCore.QModelIndex()))

    def _set_current_no_update(self, idx: QtCore.QModelIndex) -> None:
        sm = self._selection_model()
        if sm is None:
            return
        sm.setCurrentIndex(idx, QtCore.QItemSelectionModel.SelectionFlag.NoUpdate)

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.MouseButton.MiddleButton:
            if not self._autoscroll_active:
                self._start_autoscroll(event)
            event.accept()
            return
        if self._autoscroll_active:
            self._stop_autoscroll()
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            self._pending_preserved_drag_rows = []
            self._pending_preserved_drag_pos = None
            pos = event.position().toPoint()
            if self._inline_editor is not None:
                editor = self._inline_editor
                if editor is not None and not editor.geometry().contains(pos):
                    self._suppress_frame_handling_popup_once = True
                    editor.clearFocus()
                    event.accept()
                    return
            idx = self.indexAt(pos)
            if self._suppress_frame_handling_popup_once:
                self._suppress_frame_handling_popup_once = False
                if idx.isValid() and idx.column() == self.FRAME_HANDLING_COLUMN:
                    event.accept()
                    return
            if not idx.isValid():
                self.clearSelection()
                self.setCurrentCell(-1, -1)
                event.accept()
                return
            if idx.column() == self.FRAME_HANDLING_COLUMN:
                self._show_frame_handling_popup(idx)
                event.accept()
                return
            mods = QtWidgets.QApplication.keyboardModifiers()
            if mods == QtCore.Qt.KeyboardModifier.NoModifier:
                if self._is_row_selected(idx.row()):
                    if self._selected_row_count() > 1:
                        # Preserve multi-selection and defer drag start until move threshold.
                        self._pending_preserved_drag_rows = [r.row() for r in self.selectionModel().selectedRows()]
                        self._pending_preserved_drag_pos = pos
                        self._set_current_no_update(idx)
                        event.accept()
                        return
        super().mousePressEvent(event)

    def _show_frame_handling_popup(self, idx: QtCore.QModelIndex) -> None:
        if not idx.isValid() or idx.column() != self.FRAME_HANDLING_COLUMN:
            return
        sm = self._selection_model()
        selected_rows = sorted({row.row() for row in sm.selectedRows() if row.isValid()}) if sm is not None else []
        if idx.row() not in selected_rows:
            selected_rows = [idx.row()]
        self._frame_handling_target_rows = selected_rows
        current_text = str(idx.data(QtCore.Qt.ItemDataRole.DisplayRole) or "").strip()
        menu = QtWidgets.QMenu(self)
        menu.setObjectName("frameHandlingMenu")
        action_group = QtGui.QActionGroup(menu)
        action_group.setExclusive(True)
        for label in self.FRAME_HANDLING_OPTIONS:
            act = menu.addAction(label)
            act.setCheckable(True)
            act.setChecked(label == current_text)
            action_group.addAction(act)
        rect = self.visualRect(idx)
        chosen = menu.exec(self.viewport().mapToGlobal(rect.bottomLeft()))
        if chosen is not None:
            self.frame_handling_chosen.emit(idx.row(), chosen.text())
        else:
            self._frame_handling_target_rows = []
        self.clear_frame_handling_interaction_state()

    def mouseMoveEvent(self, event: QtGui.QMouseEvent) -> None:
        if self._autoscroll_active:
            event.accept()
            return
        if self._suppress_drag_select_until_release and (event.buttons() & QtCore.Qt.MouseButton.LeftButton):
            event.accept()
            return
        if (
            self._pending_preserved_drag_rows
            and self._pending_preserved_drag_pos is not None
            and (event.buttons() & QtCore.Qt.MouseButton.LeftButton)
        ):
            if (event.position().toPoint() - self._pending_preserved_drag_pos).manhattanLength() >= QtWidgets.QApplication.startDragDistance():
                self.startDrag(QtCore.Qt.DropAction.MoveAction)
                self._pending_preserved_drag_rows = []
                self._pending_preserved_drag_pos = None
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QtGui.QMouseEvent) -> None:
        try:
            if event.button() == QtCore.Qt.MouseButton.MiddleButton:
                self._stop_autoscroll()
                event.accept()
                return
            if event.button() == QtCore.Qt.MouseButton.LeftButton and self._pending_preserved_drag_rows:
                pos = event.position().toPoint()
                idx = self.indexAt(pos)
                if idx.isValid():
                    self._set_current_no_update(idx)
                event.accept()
                return
            super().mouseReleaseEvent(event)
        finally:
            if event.button() == QtCore.Qt.MouseButton.LeftButton:
                self._suppress_drag_select_until_release = False
                self._pending_preserved_drag_rows = []
                self._pending_preserved_drag_pos = None

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._update_inline_editor_geometry()

    def scrollContentsBy(self, dx: int, dy: int) -> None:
        super().scrollContentsBy(dx, dy)
        self._update_inline_editor_geometry()

    def hideEvent(self, event: QtGui.QHideEvent) -> None:
        self._stop_autoscroll()
        super().hideEvent(event)

    def mouseDoubleClickEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            idx = self.indexAt(event.position().toPoint())
            if idx.isValid():
                if idx.column() == self.FRAME_HANDLING_COLUMN:
                    self._show_frame_handling_popup(idx)
                    event.accept()
                    return
                if self._supports_inline_text_edit(idx):
                    self._set_current_no_update(idx)
                    self._start_inline_edit(idx)
                    event.accept()
                    return
                mods = QtWidgets.QApplication.keyboardModifiers()
                if (
                    mods == QtCore.Qt.KeyboardModifier.NoModifier
                    and self._is_row_selected(idx.row())
                    and self._selected_row_count() > 1
                ):
                    self._set_current_no_update(idx)
                    model = self.model()
                    if model is not None and bool(model.flags(idx) & QtCore.Qt.ItemFlag.ItemIsEditable):
                        self.edit(idx)
                    event.accept()
                    return
        super().mouseDoubleClickEvent(event)

    def startDrag(self, supportedActions: QtCore.Qt.DropActions) -> None:
        self._clear_drop_indicator()
        drag = QtGui.QDrag(self)
        mime = self.model().mimeData(self.selectedIndexes())
        if mime is None:
            return
        drag.setMimeData(mime)

        selection_model = self.selectionModel()
        rows = (
            sorted({idx.row() for idx in selection_model.selectedRows() if idx.isValid()})
            if selection_model is not None
            else []
        )
        count = len(rows) if rows else (1 if self.currentRow() >= 0 else 0)
        if count > 0:
            if count == 1 and rows:
                name_idx = self.model().index(rows[0], 0) if self.model() is not None else QtCore.QModelIndex()
                label = (str(name_idx.data(QtCore.Qt.ItemDataRole.DisplayRole) or "").strip()) or "Move job"
            elif count == 1:
                label = "Move job"
            else:
                label = f"Move {count} jobs"
            label = label[:48]

            font = self.font()
            fm = QtGui.QFontMetrics(font)
            text_w = fm.horizontalAdvance(label)
            w = max(96, text_w + 22)
            h = max(24, fm.height() + 10)
            dpr = 1.0
            win = self.window().windowHandle() if self.window() is not None else None
            if win is not None:
                try:
                    dpr = max(1.0, float(win.devicePixelRatio()))
                except Exception:
                    dpr = 1.0
            badge = QtGui.QPixmap(int(w * dpr), int(h * dpr))
            badge.setDevicePixelRatio(dpr)
            badge.fill(QtCore.Qt.GlobalColor.transparent)
            painter = QtGui.QPainter(badge)
            try:
                painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
                bg = QtGui.QColor(50, 50, 50, 230)
                border = QtGui.QColor(120, 120, 120, 220)
                painter.setPen(QtGui.QPen(border, 1))
                painter.setBrush(bg)
                painter.drawRoundedRect(QtCore.QRectF(0.5, 0.5, w - 1.0, h - 1.0), 6, 6)
                painter.setPen(QtGui.QColor(245, 245, 245))
                painter.drawText(
                    QtCore.QRect(10, 0, w - 20, h),
                    QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
                    label,
                )
            finally:
                painter.end()
            drag.setPixmap(badge)
            drag.setHotSpot(QtCore.QPoint(12, h // 2))

        try:
            drag.exec(supportedActions, QtCore.Qt.DropAction.MoveAction)
        finally:
            self._clear_drop_indicator()

    def dragEnterEvent(self, event: QtGui.QDragEnterEvent) -> None:
        if event.source() is self:
            event.setDropAction(QtCore.Qt.DropAction.MoveAction)
            event.accept()
            self._set_drop_indicator_from_pos(self._viewport_drag_pos())
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event: QtGui.QDragMoveEvent) -> None:
        if event.source() is self:
            event.setDropAction(QtCore.Qt.DropAction.MoveAction)
            event.accept()
            self._set_drop_indicator_from_pos(self._viewport_drag_pos())
            return
        super().dragMoveEvent(event)

    def dragLeaveEvent(self, event: QtGui.QDragLeaveEvent) -> None:
        self._clear_drop_indicator()
        super().dragLeaveEvent(event)

    def dropEvent(self, event: QtGui.QDropEvent) -> None:
        sm = self.selectionModel()
        selected_rows = (
            sorted({idx.row() for idx in sm.selectedRows() if idx.isValid()})
            if sm is not None
            else []
        )
        source_row = self.currentRow()
        if source_row < 0 and not selected_rows:
            event.ignore()
            return
        target_row = self._drop_indicator_row
        if target_row is None:
            target_row = self._drop_target_row_from_y(self._viewport_drag_pos().y())
        if len(selected_rows) > 1:
            self.rows_reordered_by_drag.emit(selected_rows, target_row)
        else:
            self.row_reordered_by_drag.emit(source_row, target_row)
        event.setDropAction(QtCore.Qt.DropAction.MoveAction)
        event.accept()
        self._clear_drop_indicator()


class QueueTreeView(QtWidgets.QTreeView):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.base_row_color = QtGui.QColor("#353535")
        self.alt_row_color = QtGui.QColor("#383838")
        self.selection_row_color = QtGui.QColor(DEFAULT_THEME["selection_row"])
        self.selection_row_alt_color = QtGui.QColor(DEFAULT_THEME["selection_row_alt"])
        self.selection_overlay_opacity = int(DEFAULT_THEME.get("selection_overlay_opacity", 95))
        self._inline_editor: SafeCommitLineEdit | None = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        self._suppress_enter_retrigger_once = False

    def _row_fill_and_overlay(self, index: QtCore.QModelIndex) -> tuple[QtGui.QColor, QtGui.QColor | None]:
        alt = bool(index.row() % 2)
        fill = QtGui.QColor(self.alt_row_color if alt else self.base_row_color)
        selection_model = self.selectionModel()
        if selection_model is None or not selection_model.isSelected(index):
            return fill, None
        overlay = QtGui.QColor(self.selection_row_alt_color if alt else self.selection_row_color)
        overlay.setAlpha(max(0, min(255, int(self.selection_overlay_opacity))))
        return fill, overlay

    def drawRow(
        self,
        painter: QtGui.QPainter,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        painter.save()
        row_rect = option.rect
        full_rect = QtCore.QRect(0, row_rect.top(), self.viewport().width(), row_rect.height())
        fill, overlay = self._row_fill_and_overlay(index)
        painter.fillRect(full_rect, fill)
        if overlay is not None:
            painter.fillRect(full_rect, overlay)
        painter.restore()
        item_rect = self.visualRect(index)
        branch_width = max(0, item_rect.left())
        if branch_width > 0:
            self.drawBranches(
                painter,
                QtCore.QRect(0, option.rect.top(), branch_width, option.rect.height()),
                index,
            )
        delegate = self.itemDelegateForIndex(index)
        if delegate is None:
            delegate = self.itemDelegate()
        if delegate is None:
            return
        item_opt = QtWidgets.QStyleOptionViewItem(option)
        item_opt.rect = item_rect
        item_opt.showDecorationSelected = False
        if item_opt.state & QtWidgets.QStyle.StateFlag.State_Selected:
            item_opt.state &= ~QtWidgets.QStyle.StateFlag.State_Selected
            item_opt.state &= ~QtWidgets.QStyle.StateFlag.State_HasFocus
        item_opt.backgroundBrush = QtGui.QBrush(QtCore.Qt.GlobalColor.transparent)
        delegate.paint(painter, item_opt, index)

    def drawBranches(
        self,
        painter: QtGui.QPainter,
        rect: QtCore.QRect,
        index: QtCore.QModelIndex,
    ) -> None:
        painter.save()
        fill, overlay = self._row_fill_and_overlay(index)
        painter.fillRect(rect, fill)
        if overlay is not None:
            painter.fillRect(rect, overlay)
        painter.restore()
        super().drawBranches(painter, rect, index)

    def _supports_inline_text_edit(self, index: QtCore.QModelIndex) -> bool:
        if not index.isValid():
            return False
        model = self.model()
        return bool(model is not None and (model.flags(index) & QtCore.Qt.ItemFlag.ItemIsEditable))

    def _inline_editor_rect(self, index: QtCore.QModelIndex) -> QtCore.QRect:
        rect = self.visualRect(index)
        left = rect.left()
        width = max(80, self.viewport().width() - left - 8)
        return QtCore.QRect(left, rect.top(), width, rect.height()).adjusted(1, 1, -1, -1)

    def _update_inline_editor_geometry(self) -> None:
        if self._inline_editor is None or not self._inline_edit_index.isValid():
            return
        rect = self._inline_editor_rect(QtCore.QModelIndex(self._inline_edit_index))
        if not rect.isValid() or rect.width() <= 0 or rect.height() <= 0:
            self._cancel_inline_edit()
            return
        self._inline_editor.setGeometry(rect)

    def _start_inline_edit(self, index: QtCore.QModelIndex) -> bool:
        if not self._supports_inline_text_edit(index):
            return False
        if self._inline_edit_index.isValid() and QtCore.QModelIndex(self._inline_edit_index) == index and self._inline_editor is not None:
            self._inline_editor.setFocus()
            self._inline_editor.selectAll()
            return True
        self._cancel_inline_edit()
        self._inline_edit_index = QtCore.QPersistentModelIndex(index)
        editor = SafeCommitLineEdit(self.viewport())
        editor.setText(str(index.data(QtCore.Qt.ItemDataRole.EditRole) or index.data(QtCore.Qt.ItemDataRole.DisplayRole) or ""))
        editor.commit_requested.connect(self._commit_inline_edit)
        editor.cancel_requested.connect(self._cancel_inline_edit)
        self._inline_editor = editor
        self._update_inline_editor_geometry()
        editor.show()
        editor.setFocus(QtCore.Qt.FocusReason.OtherFocusReason)
        editor.selectAll()
        return True

    @staticmethod
    def _should_start_inline_edit(
        trigger: QtWidgets.QAbstractItemView.EditTrigger,
        event: QtCore.QEvent | None,
    ) -> bool:
        if trigger in {
            QtWidgets.QAbstractItemView.EditTrigger.CurrentChanged,
            QtWidgets.QAbstractItemView.EditTrigger.SelectedClicked,
            QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers,
        }:
            return False
        if trigger in {
            QtWidgets.QAbstractItemView.EditTrigger.DoubleClicked,
            QtWidgets.QAbstractItemView.EditTrigger.EditKeyPressed,
            QtWidgets.QAbstractItemView.EditTrigger.AnyKeyPressed,
        }:
            return True
        if trigger == QtWidgets.QAbstractItemView.EditTrigger.AllEditTriggers:
            return isinstance(event, (QtGui.QMouseEvent, QtGui.QKeyEvent))
        return False

    def _commit_inline_edit(self, text: str) -> None:
        editor = self._inline_editor
        index = QtCore.QModelIndex(self._inline_edit_index)
        self._inline_editor = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        if isinstance(editor, SafeCommitLineEdit) and editor.committed_via_enter():
            self._suppress_enter_retrigger_once = True
        if editor is not None:
            editor.hide()
            editor.deleteLater()
        model = self.model()
        if model is None or not index.isValid():
            return
        model.setData(index, text, QtCore.Qt.ItemDataRole.EditRole)

    def _cancel_inline_edit(self) -> None:
        editor = self._inline_editor
        self._inline_editor = None
        self._inline_edit_index = QtCore.QPersistentModelIndex()
        if editor is not None:
            editor.hide()
            editor.deleteLater()

    def edit(
        self,
        index: QtCore.QModelIndex,
        trigger: QtWidgets.QAbstractItemView.EditTrigger = QtWidgets.QAbstractItemView.EditTrigger.AllEditTriggers,
        event: QtCore.QEvent | None = None,
    ) -> bool:
        if self._should_start_inline_edit(trigger, event) and self._start_inline_edit(index):
            return True
        return False

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        if self._suppress_enter_retrigger_once and event.key() in {QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter}:
            self._suppress_enter_retrigger_once = False
            event.accept()
            return
        idx = self.currentIndex()
        if idx.isValid():
            if event.key() in {QtCore.Qt.Key.Key_F2, QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter}:
                if self._supports_inline_text_edit(idx):
                    self._start_inline_edit(idx)
                event.accept()
                return
            if (
                not event.text().isspace()
                and event.text()
                and event.modifiers() in {QtCore.Qt.KeyboardModifier.NoModifier, QtCore.Qt.KeyboardModifier.ShiftModifier}
                and self._supports_inline_text_edit(idx)
            ):
                if self._start_inline_edit(idx) and self._inline_editor is not None:
                    self._inline_editor.setText(event.text())
                    self._inline_editor.setCursorPosition(len(event.text()))
                    event.accept()
                    return
        super().keyPressEvent(event)

    def mouseDoubleClickEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            idx = self.indexAt(event.position().toPoint())
            if idx.isValid() and self._supports_inline_text_edit(idx):
                self.setCurrentIndex(idx)
                self._start_inline_edit(idx)
                event.accept()
                return
        super().mouseDoubleClickEvent(event)

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._update_inline_editor_geometry()

    def scrollContentsBy(self, dx: int, dy: int) -> None:
        super().scrollContentsBy(dx, dy)
        self._update_inline_editor_geometry()

class QueueTableItemDelegate(QtWidgets.QStyledItemDelegate):
    STATUS_ROLE = QtCore.Qt.ItemDataRole.UserRole + 20
    _offline_stripe_brush: QtGui.QBrush | None = None

    @staticmethod
    def _selection_overlay_color(widget: QtWidgets.QWidget | None, row: int) -> QtGui.QColor | None:
        if widget is None:
            return None
        base = getattr(widget, "selection_row_alt_color", None) if (row % 2) else getattr(widget, "selection_row_color", None)
        if not isinstance(base, QtGui.QColor):
            return None
        c = QtGui.QColor(base)
        # Overlay tint, not full replacement.
        opacity = getattr(widget, "selection_overlay_opacity", 95)
        try:
            c.setAlpha(max(0, min(255, int(opacity))))
        except Exception:
            c.setAlpha(95)
        return c

    @staticmethod
    def _paint_selection_overlay(
        painter: QtGui.QPainter,
        rect: QtCore.QRect,
        overlay: QtGui.QColor | None,
    ) -> None:
        if overlay is None:
            return
        painter.save()
        painter.fillRect(rect, overlay)
        painter.restore()

    @staticmethod
    def _offline_stripe_texture_brush() -> QtGui.QBrush:
        cached = QueueTableItemDelegate._offline_stripe_brush
        if cached is not None:
            return QtGui.QBrush(cached)

        app = QtWidgets.QApplication.instance()
        screen = app.primaryScreen() if app is not None else None
        dpr = float(screen.devicePixelRatio() if screen is not None else 1.0)

        logical_size = 16
        spacing = 8
        pixel_size = max(logical_size, int(round(logical_size * dpr)))

        pix = QtGui.QPixmap(pixel_size, pixel_size)
        pix.setDevicePixelRatio(dpr)
        pix.fill(QtCore.Qt.GlobalColor.transparent)

        painter = QtGui.QPainter(pix)
        try:
            painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
            pen = QtGui.QPen(QtGui.QColor("#5a5a5a"))
            pen.setWidthF(1.0)
            pen.setCosmetic(True)
            painter.setPen(pen)

            x = -logical_size
            while x <= logical_size * 2:
                painter.drawLine(
                    QtCore.QPointF(float(x), float(logical_size)),
                    QtCore.QPointF(float(x + logical_size), 0.0),
                )
                x += spacing
        finally:
            painter.end()

        brush = QtGui.QBrush(pix)
        QueueTableItemDelegate._offline_stripe_brush = brush
        return QtGui.QBrush(brush)

    @staticmethod
    def _paint_offline_stripes(
        painter: QtGui.QPainter,
        rect: QtCore.QRect,
    ) -> None:
        painter.save()
        painter.setClipRect(rect)
        painter.setBrushOrigin(0, 0)
        painter.fillRect(rect, QueueTableItemDelegate._offline_stripe_texture_brush())
        painter.restore()

    @staticmethod
    def _paint_path_sync_overlay(
        painter: QtGui.QPainter,
        rect: QtCore.QRect,
        widget: QtWidgets.QWidget | None,
        *,
        locked: bool,
    ) -> None:
        if not locked or not rect.isValid() or rect.height() <= 0:
            return
        progress = 0.0
        if widget is not None:
            try:
                progress = float(widget.property("pathSyncOverlayProgress") or 0.0)
            except Exception:
                progress = 0.0
        progress = max(0.0, min(1.0, progress))
        band_height = max(10, int(rect.height() * 0.9))
        travel = rect.height() + band_height
        center_y = rect.bottom() + (band_height // 2) - int(progress * travel)
        overlay_rect = QtCore.QRect(
            rect.left(),
            center_y - (band_height // 2),
            rect.width(),
            band_height,
        )
        travel_ratio = 0.0
        if rect.height() > 0:
            travel_ratio = (overlay_rect.center().y() - rect.top()) / float(rect.height())
        travel_ratio = max(0.0, min(1.0, travel_ratio))
        alpha_scale = travel_ratio
        max_alpha = int(DEFAULT_THEME.get("path_sync_overlay_opacity", 28))
        if widget is not None:
            try:
                max_alpha = int(getattr(widget, "path_sync_overlay_opacity", max_alpha))
            except Exception:
                max_alpha = int(DEFAULT_THEME.get("path_sync_overlay_opacity", 28))
        max_alpha = max(0, min(255, max_alpha))
        side_alpha = max(0, min(255, int(round(max_alpha * 0.5))))

        gradient = QtGui.QLinearGradient(overlay_rect.left(), overlay_rect.bottom(), overlay_rect.left(), overlay_rect.top())
        base = QtGui.QColor("#ffffff")

        def _color(alpha: int) -> QtGui.QColor:
            c = QtGui.QColor(base)
            c.setAlpha(max(0, min(255, int(alpha * alpha_scale))))
            return c

        gradient.setColorAt(0.0, _color(0))
        gradient.setColorAt(0.2, _color(side_alpha))
        gradient.setColorAt(0.5, _color(max_alpha))
        gradient.setColorAt(0.8, _color(side_alpha))
        gradient.setColorAt(1.0, _color(0))
        painter.save()
        painter.setClipRect(rect)
        painter.fillRect(overlay_rect, gradient)
        painter.restore()

    def paint(
        self,
        painter: QtGui.QPainter,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        opt = QtWidgets.QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        selected = bool(opt.state & QtWidgets.QStyle.StateFlag.State_Selected)
        if selected:
            opt.state &= ~QtWidgets.QStyle.StateFlag.State_Selected
            opt.state &= ~QtWidgets.QStyle.StateFlag.State_HasFocus
        overlay = self._selection_overlay_color(opt.widget, index.row()) if selected else None
        is_offline = str(index.data(self.STATUS_ROLE) or "") == "Offline"
        is_path_sync_locked = bool(index.data(PATH_SYNC_LOCKED_ROLE))
        if index.column() == QueueTableModel.PROGRESS_COLUMN:
            self._paint_split_progress(
                painter,
                opt,
                index,
                overlay,
                offline=is_offline,
                locked=is_path_sync_locked,
            )
            return
        if index.column() == QueueTableView.FRAME_HANDLING_COLUMN:
            self._paint_combo_cell(
                painter,
                opt,
                index,
                overlay,
                offline=is_offline,
                locked=is_path_sync_locked,
            )
            return

        if index.column() not in {
            QueueTableModel.NAME_COLUMN,
            QueueTableModel.HIP_COLUMN,
            QueueTableModel.ROP_COLUMN,
            QueueTableModel.OUTPUT_COLUMN,
        }:
            super().paint(painter, opt, index)
            self._paint_selection_overlay(painter, opt.rect, overlay)
            if is_offline:
                self._paint_offline_stripes(painter, opt.rect)
            self._paint_path_sync_overlay(painter, opt.rect, opt.widget, locked=is_path_sync_locked)
            return

        full_text = opt.text
        opt.text = ""
        style = opt.widget.style() if opt.widget is not None else QtWidgets.QApplication.style()
        style.drawControl(QtWidgets.QStyle.ControlElement.CE_ItemViewItem, opt, painter, opt.widget)
        self._paint_selection_overlay(painter, opt.rect, overlay)
        if is_offline:
            self._paint_offline_stripes(painter, opt.rect)
        self._paint_path_sync_overlay(painter, opt.rect, opt.widget, locked=is_path_sync_locked)

        text_rect = style.subElementRect(QtWidgets.QStyle.SubElement.SE_ItemViewItemText, opt, opt.widget)
        text_rect = text_rect.adjusted(4, 0, -4, 0)
        if text_rect.width() <= 0:
            return
        fm = opt.fontMetrics
        elided = fm.elidedText(full_text, QtCore.Qt.TextElideMode.ElideMiddle, text_rect.width())
        painter.save()
        painter.setFont(opt.font)
        painter.setPen(opt.palette.color(QtGui.QPalette.ColorRole.Text))
        painter.drawText(
            text_rect,
            QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
            elided,
        )
        painter.restore()

    def _paint_combo_cell(
        self,
        painter: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
        overlay: QtGui.QColor | None,
        *,
        offline: bool = False,
        locked: bool = False,
    ) -> None:
        style = opt.widget.style() if opt.widget is not None else QtWidgets.QApplication.style()
        bg_opt = QtWidgets.QStyleOptionViewItem(opt)
        bg_opt.text = ""
        style.drawControl(QtWidgets.QStyle.ControlElement.CE_ItemViewItem, bg_opt, painter, bg_opt.widget)
        self._paint_selection_overlay(painter, opt.rect, overlay)
        if offline:
            self._paint_offline_stripes(painter, opt.rect)
        self._paint_path_sync_overlay(painter, opt.rect, opt.widget, locked=locked)
        widget = opt.widget
        fg_brush = index.data(QtCore.Qt.ItemDataRole.ForegroundRole)
        if isinstance(fg_brush, QtGui.QBrush):
            fg = QtGui.QColor(fg_brush.color())
        else:
            fg = QtGui.QColor(getattr(widget, "combo_text_color", QtGui.QColor("#ffffff")))
        rect = opt.rect
        arrow_w = 18
        left_pad = 6
        text = str(index.data(QtCore.Qt.ItemDataRole.DisplayRole) or "")

        painter.save()
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        painter.setPen(fg)
        text_rect = QtCore.QRect(rect.left() + left_pad, rect.top(), max(0, rect.width() - arrow_w - left_pad - 2), rect.height())
        elided = opt.fontMetrics.elidedText(text, QtCore.Qt.TextElideMode.ElideRight, text_rect.width())
        painter.drawText(
            text_rect,
            QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
            elided,
        )
        arrow_rect = QtCore.QRect(rect.right() - arrow_w, rect.top(), arrow_w, rect.height())
        arrow_opt = QtWidgets.QStyleOption()
        arrow_opt.rect = arrow_rect.adjusted(1, 0, -1, 0)
        arrow_opt.palette = opt.palette
        if widget is not None:
            arrow_opt.state = QtWidgets.QStyle.StateFlag.State_Enabled
        style.drawPrimitive(QtWidgets.QStyle.PrimitiveElement.PE_IndicatorArrowDown, arrow_opt, painter, opt.widget)
        painter.restore()

    def _paint_split_progress(
        self,
        painter: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
        overlay: QtGui.QColor | None = None,
        *,
        offline: bool = False,
        locked: bool = False,
    ) -> None:
        style = opt.widget.style() if opt.widget is not None else QtWidgets.QApplication.style()
        text = opt.text
        build_pct = index.data(QtCore.Qt.ItemDataRole.UserRole + 10)
        render_pct = index.data(QtCore.Qt.ItemDataRole.UserRole + 11)

        bg_opt = QtWidgets.QStyleOptionViewItem(opt)
        bg_opt.text = ""
        style.drawControl(QtWidgets.QStyle.ControlElement.CE_ItemViewItem, bg_opt, painter, bg_opt.widget)
        self._paint_selection_overlay(painter, opt.rect, overlay)
        if offline:
            self._paint_offline_stripes(painter, opt.rect)
        self._paint_path_sync_overlay(painter, opt.rect, opt.widget, locked=locked)

        rect = style.subElementRect(QtWidgets.QStyle.SubElement.SE_ItemViewItemText, bg_opt, bg_opt.widget)
        if rect.width() <= 0 or rect.height() <= 0:
            return

        painter.save()
        try:
            painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, False)
            track_color = QtGui.QColor(45, 45, 45)
            widget = opt.widget
            build_color = QtGui.QColor(
                getattr(widget, "progress_usd_build_color", QtGui.QColor(DEFAULT_THEME.get("progress_usd_build", "#11b7b7")))
            )
            render_color = QtGui.QColor(
                getattr(widget, "progress_render_color", QtGui.QColor(DEFAULT_THEME.get("progress_render", "#2fbf4a")))
            )

            line_h = 2
            gap = 0
            total_h = line_h * 2
            y_bottom = rect.bottom()
            top_y = max(rect.top(), y_bottom - total_h + 1)
            top_rect = QtCore.QRect(rect.left(), top_y, rect.width(), line_h)
            bot_rect = QtCore.QRect(rect.left(), top_y + line_h + gap, rect.width(), line_h)

            has_build = build_pct is not None
            has_render = render_pct is not None
            if has_build:
                painter.fillRect(top_rect, track_color)
            if has_render:
                painter.fillRect(bot_rect, track_color)

            def _fill_bar(bar_rect: QtCore.QRect, pct_value: object, color: QtGui.QColor) -> None:
                try:
                    pct = int(pct_value)
                except Exception:
                    return
                pct = max(0, min(100, pct))
                if pct <= 0:
                    return
                w = int((bar_rect.width() * pct) / 100)
                if w <= 0:
                    return
                painter.fillRect(QtCore.QRect(bar_rect.left(), bar_rect.top(), w, bar_rect.height()), color)

            if has_build:
                _fill_bar(top_rect, build_pct, build_color)
            if has_render:
                _fill_bar(bot_rect, render_pct, render_color)

            painter.setPen(opt.palette.color(QtGui.QPalette.ColorRole.Text))
            elided = opt.fontMetrics.elidedText(text, QtCore.Qt.TextElideMode.ElideRight, rect.width() - 6)
            text_rect = rect.adjusted(4, 0, -2, -total_h)
            painter.drawText(text_rect, QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter, elided)
        finally:
            painter.restore()


class QueueTreeItemDelegate(QtWidgets.QStyledItemDelegate):
    def paint(
        self,
        painter: QtGui.QPainter,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        opt = QtWidgets.QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        text = str(opt.text or "")
        text_rect = QtCore.QRect(opt.rect).adjusted(0, 0, -4, 0)
        if text_rect.width() <= 0:
            return
        elided = opt.fontMetrics.elidedText(text, QtCore.Qt.TextElideMode.ElideRight, text_rect.width())
        painter.save()
        painter.setFont(opt.font)
        painter.setPen(opt.palette.color(QtGui.QPalette.ColorRole.Text))
        painter.drawText(
            text_rect,
            QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
            elided,
        )
        painter.restore()


QueueTableWidget = QueueTableView
