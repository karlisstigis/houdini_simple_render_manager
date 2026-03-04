from __future__ import annotations

import contextlib
import io
import os
import runpy
import tempfile
import unittest
from pathlib import Path


class HuskRetainedUsdHookTests(unittest.TestCase):
    def _run_hook(self, script_name: str, *, env: dict[str, str], settings: dict) -> str:
        script_path = Path(__file__).resolve().parent.parent / "houdini_scripts" / script_name
        old_env = dict(os.environ)
        os.environ.update(env)
        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer):
                runpy.run_path(str(script_path), init_globals={"settings": settings})
        finally:
            os.environ.clear()
            os.environ.update(old_env)
        return buffer.getvalue()

    def _exec_hook_without_file(self, script_name: str, *, env: dict[str, str], settings: dict) -> str:
        script_path = Path(__file__).resolve().parent.parent / "houdini_scripts" / script_name
        old_env = dict(os.environ)
        os.environ.update(env)
        buffer = io.StringIO()
        try:
            source = script_path.read_text(encoding="utf-8")
            with contextlib.redirect_stdout(buffer):
                exec(compile(source, str(script_path), "exec"), {"settings": settings, "__name__": "__main__"})
        finally:
            os.environ.clear()
            os.environ.update(old_env)
        return buffer.getvalue()

    def test_husk_pre_render_prefers_candidate_matching_configured_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir) / "usd_renders" / "test" / "CamTop_usdrender_1" / "__render__.usd"
            project_path.parent.mkdir(parents=True)
            temp_path = Path(tmpdir) / "houdini_temp" / "usd_renders" / "usdrender_1" / "__render__.usd"
            temp_path.parent.mkdir(parents=True)
            output = self._run_hook(
                "hsrm_husk_pre_render.py",
                env={
                    "HSRM_RETAIN_USD_ENABLED": "1",
                    "HSRM_REUSE_EXISTING_USD": "0",
                    "HSRM_RETAIN_USD_OUTPUT_DIR": str(Path(tmpdir) / "usd_renders" / "test" / "CamTop_$RENDERID"),
                },
                settings={
                    "paths": [str(temp_path), str(project_path)],
                },
            )
            self.assertIn(project_path.as_posix(), output)

    def test_husk_post_render_prefers_existing_candidate_matching_configured_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir) / "usd_renders" / "test" / "CamTop_usdrender_1" / "__render__.usd"
            project_path.parent.mkdir(parents=True)
            project_path.write_text("usd")
            temp_path = Path(tmpdir) / "houdini_temp" / "usd_renders" / "usdrender_1" / "__render__.usd"
            temp_path.parent.mkdir(parents=True)
            temp_path.write_text("usd")
            output = self._run_hook(
                "hsrm_husk_post_render.py",
                env={
                    "HSRM_RETAIN_USD_ENABLED": "1",
                    "HSRM_RETAIN_USD_OUTPUT_DIR": str(Path(tmpdir) / "usd_renders" / "test" / "CamTop_$RENDERID"),
                },
                settings={
                    "paths": [str(temp_path), str(project_path)],
                },
            )
            self.assertIn(f"__HSRM_RETAIN_USD__|existing||{project_path.as_posix()}", output)

    def test_husk_post_render_ignores_unrelated_existing_temp_candidate_for_configured_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            configured_dir = Path(tmpdir) / "usd_renders" / "test" / "CamTop_$RENDERID"
            temp_path = Path(tmpdir) / "houdini_temp" / "usd_renders" / "usdrender_1" / "__render__.usd"
            temp_path.parent.mkdir(parents=True)
            temp_path.write_text("usd")
            output = self._run_hook(
                "hsrm_husk_post_render.py",
                env={
                    "HSRM_RETAIN_USD_ENABLED": "1",
                    "HSRM_RETAIN_USD_OUTPUT_DIR": str(configured_dir),
                },
                settings={
                    "paths": [str(temp_path)],
                },
            )
            self.assertIn("__HSRM_RETAIN_USD__|missing||", output)

    def test_husk_pre_render_can_load_helper_without___file__(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir) / "usd_renders" / "test" / "CamTop_usdrender_1" / "__render__.usd"
            project_path.parent.mkdir(parents=True)
            helper_path = Path(__file__).resolve().parent.parent / "houdini_scripts" / "hsrm_retained_usd_paths.py"
            output = self._exec_hook_without_file(
                "hsrm_husk_pre_render.py",
                env={
                    "HSRM_RETAIN_USD_ENABLED": "1",
                    "HSRM_REUSE_EXISTING_USD": "0",
                    "HSRM_RETAIN_USD_OUTPUT_DIR": str(Path(tmpdir) / "usd_renders" / "test" / "CamTop_$RENDERID"),
                    "HSRM_RETAIN_USD_HELPER_PATH": str(helper_path),
                },
                settings={
                    "paths": [str(project_path)],
                },
            )
            self.assertIn(project_path.as_posix(), output)


if __name__ == "__main__":
    unittest.main()
