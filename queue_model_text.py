from __future__ import annotations

from typing import Any


def queue_model_display_text(
    model: Any,
    row: int,
    column: int,
    *,
    display_role: Any,
) -> str:
    if model is None:
        return ""
    index = model.index(row, column)
    if not index.isValid():
        return ""
    return str(index.data(display_role) or "").strip()
