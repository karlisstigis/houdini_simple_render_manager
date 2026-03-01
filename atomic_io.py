from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def read_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_atomic(path: Path, payload: Any, *, indent: int = 2, make_backup: bool = True) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    backup_path = path.with_name(f"{path.name}.bak")
    text = json.dumps(payload, indent=indent)
    temp_path.write_text(text, encoding="utf-8")
    try:
        if path.exists() and make_backup:
            backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        pass
    os.replace(temp_path, path)
