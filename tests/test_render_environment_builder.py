from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from queue_core.queue_models import DeviceOverrideMode
from render_core.render_environment_builder import (
    apply_device_env,
    apply_retained_usd_env,
    available_gpu_ids,
    base_render_environment,
    parse_device_selection,
    should_delete_existing_retained_usd,
    should_reuse_existing_usd,
)


class RenderEnvironmentBuilderTests(unittest.TestCase):
    def test_parse_device_selection(self) -> None:
        cpu_selected, gpu_ids = parse_device_selection("cpu,0,1,foo")
        self.assertTrue(cpu_selected)
        self.assertEqual(gpu_ids, ["0", "1"])

    def test_available_gpu_ids(self) -> None:
        ids = available_gpu_ids([{"id": "0"}, {"id": "abc"}, {"id": "2"}])
        self.assertEqual(ids, ["0", "2"])

    def test_base_render_environment(self) -> None:
        env = base_render_environment(
            mode=DeviceOverrideMode.DEFAULT,
            selection="",
            cpu_selected=False,
            single_process_render=True,
            retain_usd_enabled=False,
            retained_usd_helper_path=Path("E:/hooks/helper.py"),
        )
        self.assertEqual(env["HSRM_DEVICE_MODE"], DeviceOverrideMode.DEFAULT.value)
        self.assertEqual(env["HSRM_RENDER_ALL_FRAMES_SINGLE_PROCESS"], "1")
        self.assertEqual(env["HSRM_RETAIN_USD_ENABLED"], "0")

    def test_should_delete_existing_retained_usd(self) -> None:
        self.assertFalse(
            should_delete_existing_retained_usd(output_path="", reuse_retained_usd=True, invalid_reason="")
        )
        self.assertTrue(
            should_delete_existing_retained_usd(output_path="E:/x.usd", reuse_retained_usd=False, invalid_reason="")
        )
        self.assertTrue(
            should_delete_existing_retained_usd(output_path="E:/x.usd", reuse_retained_usd=True, invalid_reason="bad")
        )

    def test_should_reuse_existing_usd(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            existing = Path(tmpdir) / "scene.usd"
            existing.write_text("usd")
            self.assertTrue(
                should_reuse_existing_usd(
                    reuse_retained_usd=True,
                    output_path=str(existing),
                    retained_reusable=True,
                    invalid_reason="",
                )
            )
            self.assertFalse(
                should_reuse_existing_usd(
                    reuse_retained_usd=True,
                    output_path=str(existing),
                    retained_reusable=False,
                    invalid_reason="",
                )
            )

    def test_apply_retained_usd_env(self) -> None:
        env: dict[str, str] = {}
        apply_retained_usd_env(env, output_path="E:/x.usd", configured_output_dir="E:/dir", reuse_existing=True)
        self.assertEqual(env["HSRM_RETAIN_USD_OUTPUT_PATH"], "E:/x.usd")
        self.assertEqual(env["HSRM_REUSE_EXISTING_USD"], "1")

        env2: dict[str, str] = {}
        apply_retained_usd_env(env2, output_path="", configured_output_dir="E:/dir", reuse_existing=False)
        self.assertEqual(env2["HSRM_RETAIN_USD_OUTPUT_DIR"], "E:/dir")
        self.assertEqual(env2["HSRM_REUSE_EXISTING_USD"], "0")

    def test_apply_device_env(self) -> None:
        env: dict[str, str] = {}
        apply_device_env(env, mode=DeviceOverrideMode.CPU, all_gpu_ids=[], selected_gpu_ids=[], cpu_selected=True)
        self.assertEqual(env["HOUDINI_OCL_DEVICETYPE"], "CPU")
        self.assertEqual(env["CUDA_VISIBLE_DEVICES"], "-1")

        env2: dict[str, str] = {}
        apply_device_env(env2, mode=DeviceOverrideMode.ALL_GPUS, all_gpu_ids=["0", "1"], selected_gpu_ids=[], cpu_selected=False)
        self.assertEqual(env2["HOUDINI_OCL_DEVICETYPE"], "GPU")
        self.assertEqual(env2["CUDA_VISIBLE_DEVICES"], "0,1")


if __name__ == "__main__":
    unittest.main()
