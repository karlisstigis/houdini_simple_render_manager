# Houdini Simple Render Manager

Local desktop queue manager for Houdini renders (PySide6 UI + hbatch/husk workflow).

## Run

```bat
run_houdini_simple_render_manager.bat
```

Or:

```bat
python houdini_simple_render_manager.py
```

## Test

```bat
python -m unittest discover -s tests
```

## Project Layout

- `houdini_simple_render_manager.py`
  Main application window and orchestration entrypoint.
- `flows/`
  High-level orchestration helpers extracted from the main window.
- `queue_core/`
  Queue domain models, lifecycle, persistence, table/filter/progress helpers.
- `usd_core/`
  Retained USD policy/runtime/panel helpers and USD queue status helpers.
- `ui_core/`
  UI widgets and shared UI state rule helpers.
- `tests/`
  Unit tests for all modules.

## Import Conventions

- Queue modules are imported from `queue_core.*` (not top-level `queue_*` files).
- Retained USD modules are imported from `usd_core.*`.
- UI shared modules are imported from `ui_core.*`.
- Orchestration helpers are imported from `flows.*`.
- `queue_core/` is used instead of `queue/` to avoid name collision with Python's stdlib `queue` module.
