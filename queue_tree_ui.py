"""Queue tree view construction and model refresh helpers."""

from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets

from widgets import PanelFrame, QueueTreeItemDelegate, QueueTreeView


TREE_KIND_ROLE = QtCore.Qt.ItemDataRole.UserRole + 1
TREE_HIP_ROLE = QtCore.Qt.ItemDataRole.UserRole + 2
TREE_ROP_ROLE = QtCore.Qt.ItemDataRole.UserRole + 3


def build_queue_tree_panel(
    parent: QtWidgets.QWidget,
    *,
    item_changed_handler: Any,
) -> tuple[QtWidgets.QWidget, QueueTreeView, QtGui.QStandardItemModel]:
    box = QtWidgets.QGroupBox("Tree view")
    layout = QtWidgets.QVBoxLayout(box)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)

    queue_tree_frame = QtWidgets.QFrame()
    queue_tree_frame.setObjectName("treePanel")
    tree_layout = QtWidgets.QVBoxLayout(queue_tree_frame)
    tree_layout.setContentsMargins(0, 0, 0, 0)
    tree_layout.setSpacing(0)

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
    queue_tree.setEditTriggers(
        QtWidgets.QAbstractItemView.EditTrigger.DoubleClicked
        | QtWidgets.QAbstractItemView.EditTrigger.EditKeyPressed
    )
    queue_tree.setModel(queue_tree_model)
    queue_tree.setItemDelegate(QueueTreeItemDelegate(queue_tree))
    queue_tree.setRootIsDecorated(True)
    queue_tree.setItemsExpandable(True)
    queue_tree.setExpandsOnDoubleClick(False)
    queue_tree_model.itemChanged.connect(item_changed_handler)
    tree_layout.addWidget(queue_tree, 1)

    layout.addWidget(queue_tree_frame, 1)

    box.setObjectName("panelEmbeddedGroup")
    box.setTitle("")
    panel = PanelFrame("Tree view", box)
    panel.set_body_margins(0, 0, 0, 0)
    return panel, queue_tree, queue_tree_model


def refresh_queue_tree_model(
    tree: QueueTreeView | None,
    model: QtGui.QStandardItemModel | None,
    jobs: list[Any],
) -> None:
    if tree is None or model is None:
        return
    tree.setUpdatesEnabled(False)
    try:
        model.clear()
        model.setHorizontalHeaderLabels(["Tree"])
        grouped: dict[str, set[str]] = {}
        for job in jobs:
            hip = str(getattr(job, "hip_path", "") or "").strip()
            rop = str(getattr(job, "rop_path", "") or "").strip()
            if not hip:
                continue
            rop_set = grouped.setdefault(hip, set())
            if rop:
                rop_set.add(rop)

        for hip_path in sorted(grouped.keys(), key=lambda s: s.lower()):
            parent = QtGui.QStandardItem(hip_path)
            parent.setEditable(True)
            parent.setToolTip(hip_path)
            parent.setData("hip", TREE_KIND_ROLE)
            parent.setData(hip_path, TREE_HIP_ROLE)
            parent.setData("", TREE_ROP_ROLE)
            model.appendRow(parent)
            for rop_path in sorted(grouped[hip_path], key=lambda s: s.lower()):
                child = QtGui.QStandardItem(rop_path)
                child.setEditable(True)
                child.setToolTip(rop_path)
                child.setData("rop", TREE_KIND_ROLE)
                child.setData(hip_path, TREE_HIP_ROLE)
                child.setData(rop_path, TREE_ROP_ROLE)
                parent.appendRow(child)
        tree.expandAll()
    finally:
        tree.setUpdatesEnabled(True)
