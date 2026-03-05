from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


def main() -> int:
    temp_root = tempfile.TemporaryDirectory()
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    os.environ["APPDATA"] = temp_root.name

    from houdini_simple_render_manager import APP_NAME, MainWindow, create_app

    app = create_app()
    win = MainWindow()
    try:
        assert win.windowTitle() == APP_NAME
        assert win.queue_file_controller.current_queue_file_path().name == "queue.json"
        assert win.scan_coordinator.build_request_payload(hip_path="C:/tmp/test.hip")["hip_path"] == "C:/tmp/test.hip"
        win._refresh_ui_state()
        app.processEvents()
    finally:
        win.close()
        temp_root.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
