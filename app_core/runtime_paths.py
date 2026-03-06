from __future__ import annotations

import sys
from pathlib import Path


def source_root(main_file: str | Path) -> Path:
    return Path(main_file).resolve().parent


def bundle_root(main_file: str | Path) -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return source_root(main_file)


def executable_root(main_file: str | Path) -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return source_root(main_file)


def bundled_path(main_file: str | Path, *parts: str) -> Path:
    return bundle_root(main_file).joinpath(*parts)


def executable_path(main_file: str | Path, *parts: str) -> Path:
    return executable_root(main_file).joinpath(*parts)
