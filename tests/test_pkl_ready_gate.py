from __future__ import annotations

import json
import os
import pickle
import sys
import tempfile
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from apps.crypto_screener.app import series_source  # noqa: E402


class PklReadyGateTests(unittest.TestCase):
    ENV_KEYS = ("QC_PKL_CACHE_ROOT", "QC_PKL_REQUIRE_READY", "QC_PKL_REQUIRE_FRESH", "QC_PKL_READY_MIN_SYMBOLS", "QC_PKL_READY_MIN_OK_RATIO")

    def setUp(self) -> None:
        self._env_old = {k: os.environ.get(k) for k in self.ENV_KEYS}

    def tearDown(self) -> None:
        for k, v in self._env_old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_gate_blocks_until_ready_matches_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            os.environ["QC_PKL_CACHE_ROOT"] = str(t / "pkl_cache")
            os.environ["QC_PKL_REQUIRE_READY"] = "1"
            os.environ["QC_PKL_REQUIRE_FRESH"] = "0"
            os.environ["QC_PKL_READY_MIN_SYMBOLS"] = "1"
            os.environ["QC_PKL_READY_MIN_OK_RATIO"] = "0.5"

            meta_dir = t / "apps" / "crypto_screener" / "web" / "data"
            meta_dir.mkdir(parents=True, exist_ok=True)
            updated_at = "2026-03-10T13:00:00+00:00"
            (meta_dir / "meta.json").write_text(json.dumps({"updated_at": updated_at}, ensure_ascii=False), encoding="utf-8")

            root = Path(os.environ["QC_PKL_CACHE_ROOT"])
            root.mkdir(parents=True, exist_ok=True)
            payload = {
                "meta": {"market": "swap", "tail": 10, "symbols_found": 1, "symbols_ok": 1, "max_dt": updated_at},
                "tail": 10,
                "symbols": {"BTC-USDT": {"dt": [updated_at], "series": {"close": [1.0]}}},
            }
            with (root / "series_swap.pkl").open("wb") as f:
                pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

            bad_ready = {"version": 1, "snapshot_updated_at": "2026-03-10T12:00:00+00:00", "markets": {"swap": payload["meta"], "spot": {}}}
            (root / "pkl_ready.json").write_text(json.dumps(bad_ready, ensure_ascii=False), encoding="utf-8")
            s0, stale0 = series_source._load_series_from_pkl_cache(market="swap", symbol="BTC-USDT", tail=10, repo_root=t, return_stale=True)
            self.assertIsNone(s0)
            self.assertTrue(stale0)

            good_ready = {"version": 1, "snapshot_updated_at": updated_at, "markets": {"swap": payload["meta"], "spot": payload["meta"]}}
            (root / "pkl_ready.json").write_text(json.dumps(good_ready, ensure_ascii=False), encoding="utf-8")
            try:
                series_source._pkl_ready_cache["mtime_ns"] = -1
            except Exception:
                pass
            s1, stale1 = series_source._load_series_from_pkl_cache(market="swap", symbol="BTC-USDT", tail=10, repo_root=t, return_stale=True)
            self.assertIsNotNone(s1)
            self.assertFalse(stale1)


if __name__ == "__main__":
    unittest.main()
