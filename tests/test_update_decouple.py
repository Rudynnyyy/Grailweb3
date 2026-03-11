from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


class UpdateDecoupleTests(unittest.TestCase):
    ENV_KEYS = ("QC_BUILD_PKL_CACHE", "QC_PREPROCESS_CONFIG")

    def setUp(self) -> None:
        self._env_old = {k: os.environ.get(k) for k in self.ENV_KEYS}

    def tearDown(self) -> None:
        for k, v in self._env_old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_update_running_clears_before_preprocess_finishes(self) -> None:
        os.environ["QC_BUILD_PKL_CACHE"] = "0"
        os.environ["QC_PREPROCESS_CONFIG"] = str(repo_root / "数据获取" / "config.yaml")

        from apps.crypto_screener.app import web_server  # noqa: E402
        from 数据获取 import incremental_update  # noqa: E402

        with web_server.run_lock:
            web_server.update_state["running"] = False
            web_server.update_state["last_error"] = None
            web_server.update_state["last_error_preprocess"] = None
        with web_server.preprocess_lock:
            web_server.preprocess_state["running"] = False
            web_server.preprocess_state["last_error"] = None

        snap_done = threading.Event()
        pre_started = threading.Event()

        old_run_once = web_server.run_once
        old_catchup = incremental_update.run_incremental_catchup

        def fake_run_once(_paths, *, fetch: bool = True) -> None:
            time.sleep(0.05)
            snap_done.set()

        def fake_catchup(_cfg_path, *, lag_hours: int = 1, max_hours: int = 24):
            pre_started.set()
            time.sleep(0.4)
            return None

        web_server.run_once = fake_run_once
        incremental_update.run_incremental_catchup = fake_catchup
        try:
            t = threading.Thread(target=web_server._run_update, kwargs={"fetch": False}, daemon=True)
            t.start()
            self.assertTrue(snap_done.wait(timeout=2.0))
            self.assertTrue(pre_started.wait(timeout=2.0))
            with web_server.run_lock:
                running = bool(web_server.update_state.get("running"))
            with web_server.preprocess_lock:
                pre_running = bool(web_server.preprocess_state.get("running"))
            self.assertFalse(running)
            self.assertTrue(pre_running)
            t.join(timeout=3.0)
        finally:
            web_server.run_once = old_run_once
            incremental_update.run_incremental_catchup = old_catchup


if __name__ == "__main__":
    unittest.main()

