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
- `run_houdini_simple_render_manager.bat`
  Windows launcher for the app.
- `render_worker.py`, `scan_worker.py`, `gui_smoke.py`
  Root entry scripts kept for subprocess/compatibility usage.
- `app_core/`
  Shared app policies/utilities (validation, notifications, diagnostics, atomic I/O).
- `flows/`
  High-level orchestration helpers extracted from the main window.
- `houdini_core/`
  Houdini probing/scan bridge and worker-side Houdini services.
- `job_core/`
  Job properties actions/presenter/state helpers.
- `queue_core/`
  Queue domain models, lifecycle, persistence, table/filter/progress helpers.
- `render_core/`
  Render runtime/session/worker helpers.
- `usd_core/`
  Retained USD policy/runtime/panel helpers and USD queue status helpers.
- `ui_core/`
  UI widgets, theme helpers, and shared UI state rule helpers.
- `worker_core/`
  Worker protocol/client transport helpers.
- `tests/`
  Unit tests for all modules.

## Import Conventions

- Shared app helpers are imported from `app_core.*`.
- Houdini integration helpers are imported from `houdini_core.*`.
- Job property helpers are imported from `job_core.*`.
- Queue modules are imported from `queue_core.*` (not top-level `queue_*` files).
- Render runtime helpers are imported from `render_core.*`.
- Retained USD modules are imported from `usd_core.*`.
- UI shared modules are imported from `ui_core.*`.
- Worker transport/protocol helpers are imported from `worker_core.*`.
- Orchestration helpers are imported from `flows.*`.
- `queue_core/` is used instead of `queue/` to avoid name collision with Python's stdlib `queue` module.
