<div align="center">

# Houdini Simple Render Manager

**A simple local render queue for Houdini that helps recover interrupted renders and reduces manual reruns on a single workstation.**

<p>
  <img src="docs/images/app_screenshot.webp" alt="Houdini Simple Render Manager screenshot" width="980">
</p>

</div>

## Overview

Long renders can fail hours into a sequence, forcing artists to manually restart frames or recover lost progress after an interruption.

**Houdini Simple Render Manager** is a local desktop tool that provides a persistent render queue, retry support, and simpler recovery of partially completed renders. It is designed for practical **local workstation workflows** rather than distributed render farms.

Under the hood, renders are executed using Houdini's standard command-line tools (`hbatch` or `husk`), allowing the manager to fit cleanly into existing Houdini setups.

Current validation scope:

- tested with Karma
- focused on Solaris / USD workflows
- local workstation execution

## Features

- automatic retry of failed renders
- resume-friendly local render workflow
- persistent local queue between sessions
- retains USD render configuration across sessions
- background worker processes for rendering and scanning
- Karma / Solaris / USD workflow support

---

## Download

Download the latest Windows executable from the project's **[Releases](../../releases)** page.  
No Python installation is required for the prebuilt version.

The release package includes:

- `HoudiniSimpleRenderManager.exe`
- bundled worker executables
- bundled runtime assets
- bundled Houdini helper scripts

## Quick Start

1. Download the latest release zip from **[Releases](../../releases)**.
2. Extract the archive.
3. Run `HoudiniSimpleRenderManager.exe`.
4. Open **Preferences** and set the path to `hbatch.exe`.
5. Open **Preferences** and set the path to your preferred sequence player.
6. Ensure `husk` is available in your Houdini installation.

## Intended Use

This tool is designed for:

- solo Houdini artists
- powerful single-machine workstations
- Karma / Solaris pipelines

It is **not intended to replace distributed render farm managers** such as Deadline or Tractor.

## Source Build Requirements

- Python 3.12
- `PySide6`
- Houdini installed locally
- access to `hbatch.exe`, configured in Preferences
- `husk` available if using Solaris / USD rendering


## For Developers

### Run From Source

Install runtime dependencies:

```bat
pip install -r requirements.txt
```

Run:

```bat
run_houdini_simple_render_manager.bat
```

Or:

```bat
python houdini_simple_render_manager.py
```

### Build Executable

Install build dependencies:

```bat
pip install -r requirements-build.txt
```

Then build:

```bat
build_exe.bat
```

This produces an `onedir` Windows bundle under:

```text
dist/HoudiniSimpleRenderManager/
```

### Test

```bat
python -m unittest discover -s tests
```

### Project Layout

- `houdini_simple_render_manager.py`  
  Main application window and composition root.
- `run_houdini_simple_render_manager.bat`  
  Windows launcher for source-mode execution.
- `render_worker.py`, `scan_worker.py`, `gui_smoke.py`  
  Root entry scripts for subprocess and compatibility usage.
- `app_core/`  
  Shared app policies and utilities such as validation, diagnostics, atomic I/O, and runtime path helpers.
- `flows/`  
  High-level orchestration helpers extracted from the main window.
- `houdini_core/`  
  Houdini probing, scan bridge logic, Houdini-side script helpers, and worker-side scan/runtime services.
- `job_core/`  
  Job properties actions, presenters, and state helpers.
- `queue_core/`  
  Queue domain models, lifecycle, persistence, refresh logic, filtering, table helpers, and queue UI coordinators.
- `render_core/`  
  Render runtime, worker payload, output parsing, and render session helpers.
- `usd_core/`  
  Retained USD policy/runtime/panel helpers and USD queue status helpers.
- `ui_core/`  
  Shared widgets, theme support, layout policies, and splitter/layout coordinators.
- `worker_core/`  
  Worker protocol and client transport helpers.
- `tests/`  
  Unit tests covering app, queue, render, worker, UI, and packaging-related behavior.

### Architecture Notes

- `houdini_simple_render_manager.py` acts as the composition root, while most UI and state behavior is delegated to focused coordinators.
- layout and UI coordination is handled by:

```text
ui_core/window_layout_coordinator.py
ui_core/panel_splitter_coordinator.py
ui_core/layout_policies.py
```

- queue lifecycle and view state logic is handled by:

```text
queue_core/queue_view_state_coordinator.py
queue_core/queue_refresh_coordinator.py
queue_core/queue_state_coordinator.py
queue_core/queue_context_menu_coordinator.py
queue_core/queue_tree_context_menu_coordinator.py
```

- Houdini scanning and integration logic is handled by:

```text
houdini_core/tree_scan_coordinator.py
houdini_core/scan_coordinator.py
```

- packaged builds resolve bundled resources through shared runtime path helpers rather than raw source-relative paths
- notification icons are bundled from **Tabler Icons** under the MIT license (see `assets/third_party/tabler/`)

### Import Conventions

- shared helpers -> `app_core.*`
- Houdini integration -> `houdini_core.*`
- job property helpers -> `job_core.*`
- queue modules -> `queue_core.*`
- render runtime helpers -> `render_core.*`
- USD helpers -> `usd_core.*`
- UI shared modules -> `ui_core.*`
- worker transport and protocol helpers -> `worker_core.*`
- orchestration helpers -> `flows.*`

`queue_core/` is used instead of `queue/` to avoid a collision with Python's standard-library `queue` module.

## Acknowledgments

This project was developed with assistance from AI coding tools (primarily OpenAI Codex). Architecture, design decisions and functionality were directed by the author.
