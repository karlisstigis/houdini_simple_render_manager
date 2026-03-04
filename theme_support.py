from __future__ import annotations

from pathlib import Path
from typing import Any

from PySide6 import QtGui


DEFAULT_THEME: dict[str, Any] = {
    "background": "#161616",
    "panel_bg": "#242424",
    "text": "#f0f0f0",
    "button_bg": "#3a3a3a",
    "button_text": "#ffffff",
    "input_bg": "#262626",
    "input_text": "#f0f0f0",
    "text_selection_bg": "#000000",
    "text_selection_text": "#ffffff",
    "table_base": "#242424",
    "table_alt": "#2a2a2a",
    "selection_line": "#8ab4f8",
    "selection_row": "#a6b9cf",
    "selection_row_alt": "#cde4ff",
    "selection_overlay_opacity": 50,
    "selection_line_enabled": True,
    "selection_line_thickness": 1,
    "queue_running": "#3e5e5d",
    "queue_done": "#293129",
    "queue_failed": "#5a3000",
    "lock_color": "#c55e0f",
    "progress_usd_build": "#1e8ecb",
    "progress_render": "#ff7700",
    "panel_gap": 8,
}


def normalize_theme_colors(theme: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(DEFAULT_THEME)
    if not isinstance(theme, dict):
        return merged
    numeric_keys = {"panel_gap", "selection_line_thickness", "selection_overlay_opacity"}
    bool_keys = {"selection_line_enabled"}
    for key in merged:
        value = theme.get(key, merged[key])
        if key in bool_keys:
            merged[key] = bool(value)
            continue
        if key in numeric_keys:
            try:
                if key == "panel_gap":
                    merged[key] = max(2, min(24, int(value)))
                elif key == "selection_overlay_opacity":
                    merged[key] = max(0, min(255, int(value)))
                else:
                    merged[key] = max(0, min(6, int(value)))
            except Exception:
                merged[key] = int(DEFAULT_THEME[key])
            continue
        if isinstance(value, str) and QtGui.QColor(value).isValid():
            merged[key] = QtGui.QColor(value).name()
    return merged


def ensure_theme_icons(icons_dir: Path, theme: dict[str, str]) -> dict[str, str]:
    icons_dir.mkdir(parents=True, exist_ok=True)

    def _icon_path(name: str) -> Path:
        return icons_dir / f"{name}.svg"

    def _url(path: Path) -> str:
        return path.as_posix()

    def _write_icon(path: Path, content: str) -> None:
        try:
            path.write_text(content, encoding="utf-8")
        except PermissionError:
            if not path.exists():
                raise
        except OSError:
            if not path.exists():
                raise

    t = normalize_theme_colors(theme)
    button_text = QtGui.QColor(t["button_text"]).name()

    combo = _icon_path("combo_down")
    spin_up = _icon_path("spin_up")
    spin_down = _icon_path("spin_down")
    splitter_grip_h = _icon_path("splitter_grip_h")
    splitter_grip_v = _icon_path("splitter_grip_v")
    lock_orange = _icon_path("lock_orange")
    dot_red = _icon_path("override_dot_red")
    tree_closed = _icon_path("tree_closed")
    tree_open = _icon_path("tree_open")
    _write_icon(
        combo,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'>"
            f"<path d='M2 3.5 L5 6.5 L8 3.5' fill='none' stroke='{button_text}' stroke-width='1.6' "
            "stroke-linecap='round' stroke-linejoin='round'/></svg>"
        ),
    )
    _write_icon(
        spin_up,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='8' height='8' viewBox='0 0 8 8'>"
            f"<path d='M1.5 5.2 L4 2.7 L6.5 5.2' fill='none' stroke='{button_text}' stroke-width='1.4' "
            "stroke-linecap='round' stroke-linejoin='round'/></svg>"
        ),
    )
    _write_icon(
        spin_down,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='8' height='8' viewBox='0 0 8 8'>"
            f"<path d='M1.5 2.8 L4 5.3 L6.5 2.8' fill='none' stroke='{button_text}' stroke-width='1.4' "
            "stroke-linecap='round' stroke-linejoin='round'/></svg>"
        ),
    )
    grip_color = QtGui.QColor(button_text).lighter(115).name()
    _write_icon(
        splitter_grip_h,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='12' height='24' viewBox='0 0 12 24'>"
            f"<circle cx='6' cy='7' r='1.1' fill='{grip_color}'/>"
            f"<circle cx='6' cy='12' r='1.1' fill='{grip_color}'/>"
            f"<circle cx='6' cy='17' r='1.1' fill='{grip_color}'/>"
            "</svg>"
        ),
    )
    _write_icon(
        splitter_grip_v,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='24' height='12' viewBox='0 0 24 12'>"
            f"<circle cx='7' cy='6' r='1.1' fill='{grip_color}'/>"
            f"<circle cx='12' cy='6' r='1.1' fill='{grip_color}'/>"
            f"<circle cx='17' cy='6' r='1.1' fill='{grip_color}'/>"
            "</svg>"
        ),
    )

    def _hex(name: str, fallback: str) -> str:
        return QtGui.QColor(t.get(name, fallback)).name()

    border = _hex("button_text", "#ffffff")
    fill = _hex("button_bg", "#3a3a3a")
    check = _hex("button_text", "#ffffff")
    border_dis = "#5a5a5a"
    fill_dis = "#2f2f2f"
    check_dis = "#8a8a8a"

    def _checkbox_svg(box_fill: str, box_stroke: str, tick: str | None) -> str:
        tick_path = ""
        if tick:
            tick_path = (
                f"<path d='M3.1 7.5 L5.6 10.0 L10.9 4.7' fill='none' stroke='{tick}' "
                "stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'/>"
            )
        return (
            "<svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14'>"
            f"<rect x='1' y='1' width='12' height='12' rx='3' ry='3' fill='{box_fill}' stroke='{box_stroke}' stroke-width='1'/>"
            f"{tick_path}</svg>"
        )

    _write_icon(_icon_path("checkbox_unchecked"), _checkbox_svg(fill, border, None))
    _write_icon(_icon_path("checkbox_checked"), _checkbox_svg(fill, border, check))
    _write_icon(_icon_path("checkbox_unchecked_disabled"), _checkbox_svg(fill_dis, border_dis, None))
    _write_icon(_icon_path("checkbox_checked_disabled"), _checkbox_svg(fill_dis, border_dis, check_dis))

    dot = _hex("button_text", "#ffffff")
    dot_dis = "#8a8a8a"

    def _radio_svg(circle_fill: str, circle_stroke: str, inner_dot: str | None) -> str:
        dot_svg = f"<circle cx='7' cy='7' r='2.2' fill='{inner_dot}'/>" if inner_dot else ""
        return (
            "<svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14'>"
            f"<circle cx='7' cy='7' r='5.5' fill='{circle_fill}' stroke='{circle_stroke}' stroke-width='1'/>"
            f"{dot_svg}</svg>"
        )

    _write_icon(_icon_path("radio_unchecked"), _radio_svg(fill, border, None))
    _write_icon(_icon_path("radio_checked"), _radio_svg(fill, border, dot))
    _write_icon(_icon_path("radio_unchecked_disabled"), _radio_svg(fill_dis, border_dis, None))
    _write_icon(_icon_path("radio_checked_disabled"), _radio_svg(fill_dis, border_dis, dot_dis))

    lock_stroke = _hex("lock_color", "#f0a020")
    lock_fill = QtGui.QColor(lock_stroke).darker(320).name()
    lock_dot = QtGui.QColor(lock_stroke).lighter(145).name()
    _write_icon(
        lock_orange,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'>"
            f"<path d='M3.5 5V3.9C3.5 2.52 4.62 1.4 6 1.4s2.5 1.12 2.5 2.5V5' fill='none' stroke='{lock_stroke}' "
            "stroke-width='1.2' stroke-linecap='round'/>"
            f"<rect x='2.2' y='5' width='7.6' height='5.6' rx='1.2' ry='1.2' fill='{lock_fill}' stroke='{lock_stroke}' stroke-width='1'/>"
            f"<circle cx='6' cy='7.8' r='0.7' fill='{lock_dot}'/>"
            "</svg>"
        ),
    )
    _write_icon(
        dot_red,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'>"
            "<circle cx='5' cy='5' r='1.2' fill='#cc3b3b'/>"
            "</svg>"
        ),
    )
    tree_arrow = "#d6d6d6"
    _write_icon(
        tree_closed,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'>"
            f"<path d='M2.6 1.9 L7.6 5 L2.6 8.1 Z' fill='{tree_arrow}'/>"
            "</svg>"
        ),
    )
    _write_icon(
        tree_open,
        (
            "<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'>"
            f"<path d='M1.9 2.6 L8.1 2.6 L5 7.6 Z' fill='{tree_arrow}'/>"
            "</svg>"
        ),
    )

    return {
        "combo_down": _url(combo),
        "spin_up": _url(spin_up),
        "spin_down": _url(spin_down),
        "splitter_grip_h": _url(splitter_grip_h),
        "splitter_grip_v": _url(splitter_grip_v),
        "checkbox_unchecked": _url(_icon_path("checkbox_unchecked")),
        "checkbox_checked": _url(_icon_path("checkbox_checked")),
        "checkbox_unchecked_disabled": _url(_icon_path("checkbox_unchecked_disabled")),
        "checkbox_checked_disabled": _url(_icon_path("checkbox_checked_disabled")),
        "radio_unchecked": _url(_icon_path("radio_unchecked")),
        "radio_checked": _url(_icon_path("radio_checked")),
        "radio_unchecked_disabled": _url(_icon_path("radio_unchecked_disabled")),
        "radio_checked_disabled": _url(_icon_path("radio_checked_disabled")),
        "lock_orange": _url(lock_orange),
        "override_dot_red": _url(dot_red),
        "tree_closed": _url(tree_closed),
        "tree_open": _url(tree_open),
    }


def build_app_stylesheet(theme: dict[str, str], icons: dict[str, str]) -> str:
    t = normalize_theme_colors(theme)
    panel_header_bg = QtGui.QColor(t["panel_bg"]).lighter(120).name()
    panel_border = QtGui.QColor(t["panel_bg"]).lighter(135).name()
    sel_row_rgba = QtGui.QColor(t["selection_row"])
    sel_row_alt_rgba = QtGui.QColor(t["selection_row_alt"])
    try:
        sel_alpha = max(0, min(255, int(t.get("selection_overlay_opacity", 95))))
    except Exception:
        sel_alpha = 95
    sel_row_rgba.setAlpha(sel_alpha)
    sel_row_alt_rgba.setAlpha(sel_alpha)
    sel_row_css = sel_row_rgba.name(QtGui.QColor.NameFormat.HexArgb)
    sel_row_alt_css = sel_row_alt_rgba.name(QtGui.QColor.NameFormat.HexArgb)
    return f"""
        QWidget {{
            background-color: {t['background']};
            color: {t['text']};
        }}
        QFrame#panelFrame {{
            background-color: {t['panel_bg']};
            border: 1px solid {panel_border};
            border-radius: 6px;
        }}
        QFrame#jobPropertiesFrame {{
            background-color: {t['panel_bg']};
            border-top: none;
            border-right: none;
            border-bottom: none;
            border-left: 1px solid {panel_border};
            border-radius: 0px;
        }}
        QWidget#panelFrameHeader {{
            background-color: {panel_header_bg};
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
            border-bottom: 1px solid {panel_border};
        }}
        QFrame#jobPropertiesFrame QWidget#panelFrameHeader {{
            border-top-left-radius: 0px;
            border-top-right-radius: 0px;
        }}
        QWidget#collapsibleSection {{
            background: transparent;
            border: none;
        }}
        QWidget#collapsibleSection > QWidget {{
            background: transparent;
            border: none;
        }}
        QToolButton#collapsibleSectionHeader {{
            background-color: {panel_header_bg};
            color: {t['text']};
            border: none;
            border-bottom: 1px solid {panel_border};
            padding: 8px 12px;
            text-align: left;
        }}
        QToolButton#collapsibleSectionHeader:hover {{
            background-color: {QtGui.QColor(panel_header_bg).lighter(108).name()};
        }}
        QToolButton#collapsibleSectionHeader:checked {{
            background-color: {panel_header_bg};
        }}
        QLabel#panelFrameTitle {{
            background-color: transparent;
            color: {t['text']};
            font-weight: 500;
        }}
        QLabel#parameterLabel {{
            background-color: transparent;
            color: #b8b8b8;
        }}
        QWidget#panelFrameBody {{
            background-color: transparent;
            border: none;
        }}
        QGroupBox {{
            background-color: {t['panel_bg']};
            border: 1px solid #3b3b3b;
            border-radius: 6px;
            margin-top: 10px;
            padding-top: 10px;
        }}
        QGroupBox#panelEmbeddedGroup {{
            background-color: transparent;
            border: none;
            margin-top: 0px;
            padding-top: 0px;
        }}
        QGroupBox#panelEmbeddedGroup::title {{
            subcontrol-origin: margin;
            color: transparent;
            background: transparent;
            border: none;
            margin: 0px;
            padding: 0px;
            height: 0px;
        }}
        QGroupBox::title {{
            subcontrol-origin: margin;
            subcontrol-position: top left;
            left: 10px;
            top: 0px;
            padding: 0 6px;
            color: {t['text']};
            background-color: transparent;
            border: none;
        }}
        QSplitter::handle {{
            background: transparent;
        }}
        QSplitter::handle:horizontal {{
            image: url({icons.get('splitter_grip_h', '')});
            image-position: center;
        }}
        QSplitter::handle:vertical {{
            image: url({icons.get('splitter_grip_v', '')});
            image-position: center;
        }}
        QSplitter::handle:hover {{
            background-color: rgba(255,255,255,0.03);
        }}
        QSplitter::handle:pressed {{
            background-color: rgba(255,255,255,0.05);
        }}
        QPushButton {{
            background-color: {t['button_bg']};
            color: {t['button_text']};
            border: 1px solid #555;
            border-radius: 3px;
            padding: 4px 8px;
        }}
        QPushButton:disabled {{
            color: #8a8a8a;
            background-color: #2f2f2f;
        }}
        QLineEdit, QSpinBox {{
            background-color: {t['button_bg']};
            color: {t['button_text']};
            border: 1px solid #555;
            border-radius: 3px;
            padding: 3px 8px;
            selection-background-color: {t['text_selection_bg']};
            selection-color: {t['text_selection_text']};
        }}
        QLineEdit:disabled, QSpinBox:disabled {{
            color: #8a8a8a;
            background-color: #2f2f2f;
            border: 1px solid #444;
        }}
        QPlainTextEdit {{
            background-color: {t['input_bg']};
            color: {t['input_text']};
            border: 1px solid #444;
            selection-background-color: {t['text_selection_bg']};
            selection-color: {t['text_selection_text']};
        }}
        QListWidget, QListView {{
            background-color: {t['button_bg']};
            color: {t['button_text']};
            border: 1px solid #555;
            border-radius: 3px;
            outline: 0;
            padding: 2px;
            selection-background-color: #4a4a4a;
            selection-color: #ffffff;
        }}
        QListWidget::item, QListView::item {{
            padding: 4px 6px;
            border-radius: 2px;
            background: transparent;
        }}
        QListWidget::item:selected, QListView::item:selected,
        QListWidget::item:selected:active, QListView::item:selected:active,
        QListWidget::item:selected:!active, QListView::item:selected:!active {{
            background-color: {t['selection_row']};
            color: #ffffff;
        }}
        QListWidget::item:hover, QListView::item:hover {{
            background-color: #3a3a3a;
        }}
        QListWidget:disabled, QListView:disabled {{
            color: #8a8a8a;
            background-color: #2f2f2f;
            border: 1px solid #444;
        }}
        QSpinBox {{
            padding-right: 22px;
        }}
        QSpinBox::up-button, QSpinBox::down-button {{
            width: 18px;
            border: none;
            border-left: 1px solid #555;
            background-color: transparent;
        }}
        QSpinBox::up-button {{
            subcontrol-origin: border;
            subcontrol-position: top right;
            border-top-right-radius: 3px;
        }}
        QSpinBox::down-button {{
            subcontrol-origin: border;
            subcontrol-position: bottom right;
            border-bottom-right-radius: 3px;
            border-top: 1px solid #555;
        }}
        QSpinBox::up-arrow {{
            image: url({icons.get('spin_up', '')});
            width: 8px;
            height: 8px;
        }}
        QSpinBox::down-arrow {{
            image: url({icons.get('spin_down', '')});
            width: 8px;
            height: 8px;
        }}
        QComboBox {{
            background-color: {t['button_bg']};
            color: {t['button_text']};
            border: 1px solid #555;
            border-radius: 3px;
            padding: 3px 28px 3px 8px;
        }}
        QComboBox:disabled {{
            color: #8a8a8a;
            background-color: #2f2f2f;
            border: 1px solid #444;
        }}
        QComboBox::drop-down {{
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 22px;
            border: none;
            border-left: 1px solid #555;
            background-color: transparent;
            border-top-right-radius: 3px;
            border-bottom-right-radius: 3px;
        }}
        QComboBox::down-arrow {{
            image: url({icons.get('combo_down', '')});
            width: 9px;
            height: 9px;
        }}
        QComboBox QAbstractItemView {{
            background-color: {t['input_bg']};
            color: {t['input_text']};
            border: 1px solid #555;
            selection-background-color: {t['text_selection_bg']};
            selection-color: {t['text_selection_text']};
            outline: 0;
        }}
        QCheckBox {{
            spacing: 6px;
        }}
        QCheckBox::indicator {{
            width: 14px;
            height: 14px;
            image: url({icons.get('checkbox_unchecked', '')});
            border: none;
            background: transparent;
        }}
        QCheckBox::indicator:unchecked:disabled {{
            image: url({icons.get('checkbox_unchecked_disabled', '')});
        }}
        QCheckBox::indicator:checked {{
            image: url({icons.get('checkbox_checked', '')});
        }}
        QCheckBox::indicator:checked:disabled {{
            image: url({icons.get('checkbox_checked_disabled', '')});
        }}
        QRadioButton {{
            spacing: 6px;
        }}
        QRadioButton::indicator {{
            width: 14px;
            height: 14px;
            image: url({icons.get('radio_unchecked', '')});
            border: none;
            background: transparent;
        }}
        QRadioButton::indicator:unchecked:disabled {{
            image: url({icons.get('radio_unchecked_disabled', '')});
        }}
        QRadioButton::indicator:checked {{
            image: url({icons.get('radio_checked', '')});
        }}
        QRadioButton::indicator:checked:disabled {{
            image: url({icons.get('radio_checked_disabled', '')});
        }}
        QLabel, QCheckBox, QRadioButton {{
            background-color: transparent;
        }}
        QWidget#transparentHost {{
            background-color: transparent;
        }}
        QFrame#ropPanel {{
            background-color: {t['panel_bg']};
            border: 1px solid #555;
            border-radius: 6px;
        }}
        QFrame#treePanel {{
            background-color: transparent;
            border: none;
            border-radius: 0px;
        }}
        QFrame#toolbarSeparator {{
            background-color: {panel_border};
            border: none;
            min-width: 1px;
            max-width: 1px;
            margin-top: 2px;
            margin-bottom: 2px;
        }}
        QFrame#jobPropertiesSeparator {{
            background-color: {panel_border};
            border: none;
            min-height: 1px;
            max-height: 1px;
        }}
        QLineEdit#jobPropertiesReadOnlyField {{
            color: #9a9a9a;
        }}
        QWidget#ropPanelHeader {{
            background-color: {panel_header_bg};
            border: none;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
            border-bottom: 1px solid {panel_border};
        }}
        QLabel#ropPanelTitle {{
            background-color: transparent;
            color: {t['text']};
            font-weight: 500;
        }}
        QListWidget#ropList {{
            background-color: #353535;
            alternate-background-color: #383838;
            color: {t['button_text']};
            border: none;
            border-radius: 0px;
            outline: 0;
            padding: 0px;
            selection-background-color: #575757;
            selection-color: #ffffff;
        }}
        QListWidget#ropList::item {{
            padding: 7px 10px;
            border-radius: 0px;
            background: transparent;
        }}
        QListWidget#ropList::item:selected,
        QListWidget#ropList::item:selected:active,
        QListWidget#ropList::item:selected:!active {{
            background-color: {sel_row_css};
            color: #ffffff;
        }}
        QListWidget#ropList::item:selected:alternate,
        QListWidget#ropList::item:selected:alternate:active,
        QListWidget#ropList::item:selected:alternate:!active {{
            background-color: {sel_row_alt_css};
            color: #ffffff;
        }}
        QListWidget#ropList::item:hover {{
            background-color: #4a4a4a;
        }}
        QTreeView#queueTree {{
            background-color: #353535;
            alternate-background-color: #383838;
            color: {t['button_text']};
            border: none;
            border-radius: 0px;
            outline: 0;
            padding: 0px;
            show-decoration-selected: 1;
            selection-background-color: transparent;
            selection-color: #ffffff;
        }}
        QTreeView#queueTree::item {{
            padding: 7px 10px 7px 2px;
            border: none;
            background: transparent;
        }}
        QTreeView#queueTree::item:selected,
        QTreeView#queueTree::item:selected:active,
        QTreeView#queueTree::item:selected:!active,
        QTreeView#queueTree::item:selected:alternate,
        QTreeView#queueTree::item:selected:alternate:active,
        QTreeView#queueTree::item:selected:alternate:!active {{
            background: transparent;
            color: #ffffff;
        }}
        QTreeView#queueTree::branch {{
            background: transparent;
            margin-left: 8px;
        }}
        QTreeView#queueTree::branch:has-children:closed {{
            image: url({icons.get('tree_closed', '')});
        }}
        QTreeView#queueTree::branch:has-children:open {{
            image: url({icons.get('tree_open', '')});
        }}
        QTableWidget {{
            background-color: {t['table_base']};
            alternate-background-color: {t['table_alt']};
            gridline-color: #2b2b2b;
            border: none;
            outline: 0;
        }}
        QTableWidget::item:selected {{
            background-color: {t['selection_row']};
            color: #ffffff;
        }}
        QTableWidget::item:selected:alternate {{
            background-color: {t['selection_row_alt']};
            color: #ffffff;
        }}
        QHeaderView::section {{
            background-color: #3a3a3a;
            color: {t['text']};
            border: none;
            border-right: 1px solid #4a4a4a;
            border-bottom: 1px solid #4a4a4a;
            padding: 4px;
            font-weight: normal;
        }}
        QHeaderView::section:first {{
            border-left: 1px solid #4a4a4a;
        }}
        QHeaderView::section:selected {{
            background-color: #3a3a3a;
            color: {t['text']};
            font-weight: normal;
        }}
        QHeaderView::section:checked, QHeaderView::section:pressed {{
            background-color: #3a3a3a;
            color: {t['text']};
            font-weight: normal;
        }}
        QScrollBar:vertical {{
            background: transparent;
            width: 12px;
            margin: 2px 2px 2px 2px;
        }}
        QScrollBar::handle:vertical {{
            background: #555555;
            border: 1px solid #666666;
            border-radius: 5px;
            min-height: 24px;
        }}
        QScrollBar::handle:vertical:hover {{
            background: #676767;
        }}
        QScrollBar::handle:vertical:pressed {{
            background: #7a7a7a;
        }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            background: transparent;
            border: none;
            height: 0px;
        }}
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
            background: transparent;
        }}
        QScrollBar:horizontal {{
            background: transparent;
            height: 12px;
            margin: 2px 2px 2px 2px;
        }}
        QScrollBar::handle:horizontal {{
            background: #555555;
            border: 1px solid #666666;
            border-radius: 5px;
            min-width: 24px;
        }}
        QScrollBar::handle:horizontal:hover {{
            background: #676767;
        }}
        QScrollBar::handle:horizontal:pressed {{
            background: #7a7a7a;
        }}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
            background: transparent;
            border: none;
            width: 0px;
        }}
        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{
            background: transparent;
        }}
        QMenu {{
            background-color: {t['background']};
            color: {t['text']};
            border: 1px solid #4a4a4a;
        }}
        QMenu::item {{
            padding: 4px 22px 4px 22px;
        }}
        QMenu::item:selected {{
            background-color: #3d6db3;
            color: #ffffff;
        }}
        QMenu::item:disabled {{
            color: #7a7a7a;
            background-color: transparent;
        }}
        QMenu::separator {{
            height: 1px;
            background: #565656;
            margin: 4px 8px 4px 8px;
        }}
        QStatusBar {{
            background: transparent;
            color: {t['text']};
            padding: 2px 6px 2px 6px;
        }}
        QStatusBar::item {{
            border: none;
        }}
    """
