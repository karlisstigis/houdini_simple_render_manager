"""Helpers for loading/copying Houdini-side scripts and building preflight script text."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from app_core.runtime_paths import bundled_path


REQUIRED_HOUDINI_SCRIPT_FILENAMES = [
    "hsrm_render_preflight_template.py",
    "hsrm_retained_usd_paths.py",
    "hsrm_husk_pre_render.py",
    "hsrm_husk_pre_frame.py",
    "hsrm_husk_post_frame.py",
    "hsrm_husk_post_render.py",
    "hsrm_resolve_range.py",
    "hsrm_scan_nodes.py",
]


def project_houdini_scripts_dir(main_file: str, scripts_dir_name: str) -> Path:
    return bundled_path(main_file, scripts_dir_name)


def validate_houdini_script_files(scripts_dir: Path) -> list[str]:
    return [name for name in REQUIRED_HOUDINI_SCRIPT_FILENAMES if not (scripts_dir / name).exists()]


def load_houdini_script_text(scripts_dir: Path, filename: str) -> str:
    path = scripts_dir / filename
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        raise RuntimeError(f"Failed to load {path}: {exc}") from exc


def build_render_preflight_script(
    *,
    scripts_dir: Path,
    rop_path: str,
    disable_husk_mplay: bool,
    hook_paths: dict[str, str],
) -> str:
    template = load_houdini_script_text(scripts_dir, "hsrm_render_preflight_template.py")
    replacements = {
        "__HSRM_ROP_PATH_REPR__": repr(rop_path),
        "__HSRM_ROP_PATH_TEXT__": rop_path,
        "__HSRM_DISABLE_HUSK_MPLAY__": "1" if disable_husk_mplay else "0",
        "__HSRM_HOOK_PRE_RENDER_REPR__": repr(hook_paths.get("pre_render", "").replace("\\", "/")),
        "__HSRM_HOOK_PRE_FRAME_REPR__": repr(hook_paths.get("pre_frame", "").replace("\\", "/")),
        "__HSRM_HOOK_POST_FRAME_REPR__": repr(hook_paths.get("post_frame", "").replace("\\", "/")),
        "__HSRM_HOOK_POST_RENDER_REPR__": repr(hook_paths.get("post_render", "").replace("\\", "/")),
    }
    for key, value in replacements.items():
        template = template.replace(key, value)
    return template


def ensure_husk_hook_files(
    *,
    scripts_dir: Path,
    hook_script_path_fn: Callable[[str], Path],
) -> dict[str, str]:
    helper = hook_script_path_fn("hsrm_retained_usd_paths")
    pre_render = hook_script_path_fn("hsrm_husk_pre_render")
    pre_frame = hook_script_path_fn("hsrm_husk_pre_frame")
    post_frame = hook_script_path_fn("hsrm_husk_post_frame")
    post_render = hook_script_path_fn("hsrm_husk_post_render")

    helper.write_text(load_houdini_script_text(scripts_dir, "hsrm_retained_usd_paths.py"), encoding="utf-8")
    pre_render.write_text(load_houdini_script_text(scripts_dir, "hsrm_husk_pre_render.py"), encoding="utf-8")
    pre_frame.write_text(load_houdini_script_text(scripts_dir, "hsrm_husk_pre_frame.py"), encoding="utf-8")
    post_frame.write_text(load_houdini_script_text(scripts_dir, "hsrm_husk_post_frame.py"), encoding="utf-8")
    post_render.write_text(load_houdini_script_text(scripts_dir, "hsrm_husk_post_render.py"), encoding="utf-8")

    return {
        "pre_render": str(pre_render),
        "pre_frame": str(pre_frame),
        "post_frame": str(post_frame),
        "post_render": str(post_render),
    }


def ensure_runtime_script_copy(
    *,
    scripts_dir: Path,
    source_filename: str,
    target_stem: str,
    hook_script_path_fn: Callable[[str], Path],
) -> Path:
    path = hook_script_path_fn(target_stem)
    path.write_text(load_houdini_script_text(scripts_dir, source_filename), encoding="utf-8")
    return path
