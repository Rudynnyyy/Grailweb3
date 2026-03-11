from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


class FactorCacheReuseByDtTests(unittest.TestCase):
    ENV_KEYS = ("QC_MERGE_SWAP_PATH", "QC_MERGE_SPOT_PATH", "QC_SCREENER_FALLBACK_SWAP_DIR", "QC_DATA_CENTER_ROOT", "QC_PKL_CACHE_ROOT")

    def setUp(self) -> None:
        self._env_old = {k: os.environ.get(k) for k in self.ENV_KEYS}

    def tearDown(self) -> None:
        for k, v in self._env_old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_reuse_when_dt_not_advanced_even_if_mtime_changes(self) -> None:
        from 数据获取.factor_cache_update import build_market_cache  # noqa: E402

        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            merge_swap = t / "merge_swap"
            merge_swap.mkdir(parents=True, exist_ok=True)
            dc_root = t / "data_center"
            (dc_root / "kline" / "swap").mkdir(parents=True, exist_ok=True)
            pkl_root = t / "pkl_cache"
            pkl_root.mkdir(parents=True, exist_ok=True)

            os.environ["QC_MERGE_SWAP_PATH"] = str(merge_swap)
            os.environ["QC_DATA_CENTER_ROOT"] = str(dc_root)
            os.environ["QC_PKL_CACHE_ROOT"] = str(pkl_root)

            header = "candle_begin_time,open,high,low,close,volume,quote_volume"
            body = []
            for i in range(30):
                ts = f"2026-03-11T{(i%24):02d}:00:00+00:00"
                body.append(f"{ts},1,1,1,1,1,1")
            rows = [header] + body
            csv_path = merge_swap / "BTC-USDT.csv"
            _write_csv(csv_path, rows)

            r1 = build_market_cache(market="swap", tail=20, symbols_limit=0, workers=1, incremental=True)
            self.assertTrue(bool(r1.get("ok")))
            meta1 = (r1 or {}).get("meta") or {}
            self.assertEqual(int(meta1.get("symbols_ok") or 0), 1)

            time.sleep(0.02)
            _write_csv(csv_path, rows)

            r2 = build_market_cache(market="swap", tail=20, symbols_limit=0, workers=1, incremental=True)
            self.assertTrue(bool(r2.get("ok")))
            meta2 = (r2 or {}).get("meta") or {}
            self.assertEqual(int(meta2.get("symbols_ok") or 0), 1)
            self.assertGreaterEqual(int(meta2.get("symbols_reused_by_dt") or 0), 1)


if __name__ == "__main__":
    unittest.main()
