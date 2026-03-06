"""Queue tree view construction and model refresh helpers."""

from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets

from ui_core.widgets import PanelFrame, QueueTreeItemDelegate, QueueTreeView


TREE_KIND_ROLE = QtCore.Qt.ItemDataRole.UserRole + 1
TREE_HIP_ROLE = QtCore.Qt.ItemDataRole.UserRole + 2
TREE_ROP_ROLE = QtCore.Qt.ItemDataRole.UserRole + 3
TREE_USED_ROLE = QtCore.Qt.ItemDataRole.UserRole + 4


def build_queue_tree_panel(
    parent: QtWidgets.QWidget,
    *,
    item_changed_handler: Any,
) -> tuple[QtWidgets.QWidget, QueueTreeView, QtGui.QStandardItemModel, QtWidgets.QPushButton, QtWidgets.QCheckBox]:
    box = QtWidgets.QGroupBox("Tree view")
    layout = QtWidgets.QVBoxLayout(box)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)

    queue_tree_frame = QtWidgets.QFrame()
    queue_tree_frame.setObjectName("treePanel")
    tree_layout = QtWidgets.QVBoxLayout(queue_tree_frame)
    tree_layout.setContentsMargins(0, 0, 0, 0)
    tree_layout.setSpacing(0)

    header = QtWidgets.QWidget()
    header.setObjectName("transparentHost")
    header_layout = QtWidgets.QHBoxLayout(header)
    header_layout.setContentsMargins(8, 8, 8, 8)
    header_layout.setSpacing(8)
    reload_all_button = QtWidgets.QPushButton("Reload All")
    header_layout.addWidget(reload_all_button)
    header_layout.addStretch(1)
    show_used_only_checkbox = QtWidgets.QCheckBox("Show Used Only")
    show_used_only_checkbox.setChecked(True)
    show_used_only_checkbox.setLayoutDirection(QtCore.Qt.LayoutDirection.RightToLeft)
    header_layout.addWidget(show_used_only_checkbox)
    header_min_w = (
        int(header_layout.contentsMargins().left())
        + int(reload_all_button.sizeHint().width())
        + int(header_layout.spacing())
        + int(show_used_only_checkbox.sizeHint().width())
        + int(header_layout.contentsMargins().right())
    )
    header.setMinimumWidth(max(1, int(header_min_w)))
    tree_layout.addWidget(header)

    queue_tree_model = QtGui.QStandardItemModel(parent)
    queue_tree_model.setHorizontalHeaderLabels(["Tree"])

    queue_tree = QueueTreeView()
    queue_tree.setObjectName("queueTree")
    queue_tree.setHeaderHidden(True)
    queue_tree.setUniformRowHeights(True)
    queue_tree.setIndentation(24)
    queue_tree.setAlternatingRowColors(True)
    queue_tree.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
    queue_tree.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
    queue_tree.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
    queue_tree.setModel(queue_tree_model)
    queue_tree.setItemDelegate(QueueTreeItemDelegate())
    queue_tree.setRootIsDecorated(True)
    queue_tree.setItemsExpandable(True)
    queue_tree.setExpandsOnDoubleClick(False)
    queue_tree.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
    queue_tree.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    queue_tree_model.itemChanged.connect(item_changed_handler)
    tree_layout.addWidget(queue_tree, 1)

    layout.addWidget(queue_tree_frame, 1)

    box.setObjectName("panelEmbeddedGroup")
    box.setTitle("")
    panel = PanelFrame("Tree view", box, collapsible=True)
    panel.set_body_margins(0, 0, 0, 0)
    return panel, queue_tree, queue_tree_model, reload_all_button, show_used_only_checkbox


def refresh_queue_tree_model(
    tree: QueueTreeView | None,
    model: QtGui.QStandardItemModel | None,
    jobs: list[Any],
    *,
    is_locked_job_fn=None,
    show_used_only: bool = True,
    rop_paths_for_hip_fn=None,
) -> None:
    if tree is None or model is None:
        return
    tree.setUpdatesEnabled(False)
    try:
        model.clear()
        model.setHorizontalHeaderLabels(["Tree"])
        grouped: dict[str, set[str]] = {}
        locked_hips: set[str] = set()
        locked_rops: set[tuple[str, str]] = set()
        for job in jobs:
            spec = getattr(job, "spec", None)
            hip = str((spec.hip_path if spec is not None else getattr(job, "hip_path", "")) or "").strip()
            rop = str((spec.rop_path if spec is not None else getattr(job, "rop_path", "")) or "").strip()
            if not hip:
                continue
            rop_set = grouped.setdefault(hip, set())
            if rop:
                rop_set.add(rop)
            if callable(is_locked_job_fn) and is_locked_job_fn(job):
                locked_hips.add(hip)
                if rop:
                    locked_rops.add((hip, rop))

        all_rops_by_hip: dict[str, set[str]] = {hip: set(rops) for hip, rops in grouped.items()}
        if not bool(show_used_only) and callable(rop_paths_for_hip_fn):
            for hip_path in list(all_rops_by_hip.keys()):
                try:
                    scanned_paths = list(rop_paths_for_hip_fn(hip_path) or [])
                except Exception:
                    scanned_paths = []
                for rop_path in scanned_paths:
                    rop_value = str(rop_path or "").strip()
                    if rop_value:
                        all_rops_by_hip.setdefault(hip_path, set()).add(rop_value)

        for hip_path in sorted(all_rops_by_hip.keys(), key=lambda s: s.lower()):
            parent = QtGui.QStandardItem(hip_path)
            hip_locked = hip_path in locked_hips
            parent.setEditable(not hip_locked)
            parent.setToolTip(hip_path)
            parent.setData("hip", TREE_KIND_ROLE)
            parent.setData(hip_path, TREE_HIP_ROLE)
            parent.setData("", TREE_ROP_ROLE)
            model.appendRow(parent)
            used_rops = grouped.get(hip_path, set())
            for rop_path in sorted(all_rops_by_hip[hip_path], key=lambda s: s.lower()):
                child = QtGui.QStandardItem(rop_path)
                used_in_queue = rop_path in used_rops
                child_locked = hip_locked or ((hip_path, rop_path) in locked_rops)
                child.setEditable(not child_locked and used_in_queue)
                if not used_in_queue:
                    child.setForeground(QtGui.QBrush(QtGui.QColor("#7a7a7a")))
                    child.setToolTip(f"{rop_path}\nNot used in queue.")
                else:
                    child.setToolTip(rop_path)
                child.setData("rop", TREE_KIND_ROLE)
                child.setData(hip_path, TREE_HIP_ROLE)
                child.setData(rop_path, TREE_ROP_ROLE)
                child.setData(bool(used_in_queue), TREE_USED_ROLE)
                parent.appendRow(child)
        tree.expandAll()
    finally:
        tree.setUpdatesEnabled(True)
