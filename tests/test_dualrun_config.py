from __future__ import annotations

import importlib
import sys
import unittest
from collections import deque
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

web_server = importlib.import_module("apps.crypto_screener.app.web_server")


class DualRunConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        with web_server.dualrun_lock:
            self._cfg_old = dict(web_server.dualrun_config)
            self._metrics_old = {
                "total": int(web_server.dualrun_metrics.get("total") or 0),
                "shadow_runs": int(web_server.dualrun_metrics.get("shadow_runs") or 0),
                "drift_violations": int(web_server.dualrun_metrics.get("drift_violations") or 0),
                "err_5xx": int(web_server.dualrun_metrics.get("err_5xx") or 0),
                "err_429": int(web_server.dualrun_metrics.get("err_429") or 0),
                "p95_ms": float(web_server.dualrun_metrics.get("p95_ms") or 0.0),
                "qps": float(web_server.dualrun_metrics.get("qps") or 0.0),
                "updated_at": web_server.dualrun_metrics.get("updated_at"),
            }
            web_server.dualrun_config.clear()
            web_server.dualrun_config.update(
                {"enabled": True, "primary": "legacy", "sample_ratio": 1.0, "max_symbols": 120, "split_rule": "user_hash"}
            )
            web_server.dualrun_metrics["total"] = 0
            web_server.dualrun_metrics["shadow_runs"] = 0
            web_server.dualrun_metrics["drift_violations"] = 0
            web_server.dualrun_metrics["err_5xx"] = 0
            web_server.dualrun_metrics["err_429"] = 0
            web_server.dualrun_metrics["p95_ms"] = 0.0
            web_server.dualrun_metrics["qps"] = 0.0
            web_server.dualrun_metrics["updated_at"] = None
            web_server.dualrun_metrics["lat_ms_window"] = deque(maxlen=1024)
            web_server.dualrun_metrics["ts_window"] = deque(maxlen=4096)

    def tearDown(self) -> None:
        with web_server.dualrun_lock:
            web_server.dualrun_config.clear()
            web_server.dualrun_config.update(self._cfg_old)
            for k, v in self._metrics_old.items():
                web_server.dualrun_metrics[k] = v
            if not isinstance(web_server.dualrun_metrics.get("lat_ms_window"), deque):
                web_server.dualrun_metrics["lat_ms_window"] = deque(maxlen=1024)
            if not isinstance(web_server.dualrun_metrics.get("ts_window"), deque):
                web_server.dualrun_metrics["ts_window"] = deque(maxlen=4096)

    def test_sanitize_dualrun_config(self) -> None:
        cfg = web_server._sanitize_dualrun_config(
            {"enabled": 1, "primary": "coin", "sample_ratio": 9, "max_symbols": -1, "split_rule": "x"},
            {"enabled": False, "primary": "legacy", "sample_ratio": 0.1, "max_symbols": 500, "split_rule": "user_hash"},
        )
        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["primary"], "coin")
        self.assertEqual(cfg["sample_ratio"], 1.0)
        self.assertEqual(cfg["max_symbols"], 10)
        self.assertEqual(cfg["split_rule"], "user_hash")

    def test_dualrun_record_request_metrics(self) -> None:
        web_server._dualrun_record_request(path0="/api/latest_enriched", code_i=200, dur_ms=210.0)
        web_server._dualrun_record_request(path0="/api/latest_enriched", code_i=429, dur_ms=310.0)
        web_server._dualrun_record_request(path0="/api/latest_enriched", code_i=503, dur_ms=110.0)
        with web_server.dualrun_lock:
            total = int(web_server.dualrun_metrics.get("total") or 0)
            shadow = int(web_server.dualrun_metrics.get("shadow_runs") or 0)
            err_429 = int(web_server.dualrun_metrics.get("err_429") or 0)
            err_5xx = int(web_server.dualrun_metrics.get("err_5xx") or 0)
            p95 = float(web_server.dualrun_metrics.get("p95_ms") or 0.0)
        self.assertEqual(total, 3)
        self.assertGreaterEqual(shadow, 3)
        self.assertEqual(err_429, 1)
        self.assertEqual(err_5xx, 1)
        self.assertGreater(p95, 0.0)


if __name__ == "__main__":
    unittest.main()
