import importlib.util
import os
import sys
from pathlib import Path


def _load_helper_module():
    helper_path = str(os.environ.get("HSRM_RETAIN_USD_HELPER_PATH", "") or "").strip()
    if not helper_path:
        script_file = str(globals().get("__file__", "") or "").strip()
        if script_file:
            helper_path = str(Path(script_file).resolve().with_name("hsrm_retained_usd_paths.py"))
    if helper_path:
        spec = importlib.util.spec_from_file_location("hsrm_retained_usd_paths", helper_path)
        if spec is not None and spec.loader is not None:
            module = importlib.util.module_from_spec(spec)
            sys.modules.setdefault("hsrm_retained_usd_paths", module)
            spec.loader.exec_module(module)
            return module
    import hsrm_retained_usd_paths as module

    return module


_HELPER = _load_helper_module()


def _find_real_usd_path():
    output_path, output_dir = _HELPER.retained_usd_env()
    return _HELPER.choose_retained_usd_path(
        settings_obj=globals().get("settings"),
        configured_output_path=output_path,
        configured_output_dir=output_dir,
        require_existing=False,
    )


def _report_retained_usd_plan():
    if os.environ.get("HSRM_RETAIN_USD_ENABLED") != "1":
        return
    output_path = _find_real_usd_path()
    if not output_path:
        print("__HSRM_RETAIN_USD__|missing||", flush=True)
        return
    print("__HSRM_RETAIN_USD__|planned||%s" % output_path.replace("\\", "/"), flush=True)


print("__HSRM_PHASE__|Render|start", flush=True)
_report_retained_usd_plan()
