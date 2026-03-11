from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd


preprocess_fast = importlib.import_module("数据获取.preprocess_fast")
factor_loader_mod = importlib.import_module("数据获取.factor_loader")
inc_mod = importlib.import_module("数据获取.incremental_update")


class PreprocessPipelineTests(unittest.TestCase):
    def _write_sample_csv(self, path: Path) -> None:
        df = pd.DataFrame(
            {
                "candle_begin_time": [
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                    "2026-01-01 01:00:00",
                    "2026-01-01 02:00:00",
                ],
                "open": [1.0, 2.0, 2.1, 3.0],
                "high": [1.1, 2.2, 2.2, 3.1],
                "low": [0.9, 1.9, 2.0, 2.9],
                "close": [1.05, 2.05, 2.08, 3.05],
                "volume": [10, 20, 21, 30],
                "quote_volume": [11, 22, 23, 33],
                "trade_num": [1, 2, 2, 3],
                "taker_buy_base_asset_volume": [5, 10, 10, 15],
                "taker_buy_quote_asset_volume": [6, 12, 12, 18],
            }
        )
        df.to_csv(path, index=False, encoding="utf-8")

    def test_partition_manifest_and_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            swap = root / "swap_lin"
            spot = root / "spot_lin"
            out = root / "out"
            swap.mkdir(parents=True)
            spot.mkdir(parents=True)
            self._write_sample_csv(swap / "BTC-USDT.csv")
            cfg = preprocess_fast.PreprocessConfig(
                source_swap_dir=swap,
                source_spot_dir=spot,
                output_root=out,
                file_format="pkl",
                compression="zstd",
                include_optional_indicators=True,
            )
            res = preprocess_fast.preprocess_all(cfg)
            self.assertTrue((out / "_SUCCESS").exists())
            self.assertTrue((out / "manifest.json").exists())
            self.assertGreaterEqual(res["global_manifest"]["total_rows"], 3)
            manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
            self.assertGreaterEqual(manifest["total_partitions"], 1)
            p0 = manifest["files"][0]["partition"]
            self.assertTrue((out / p0 / "manifest.json").exists())
            self.assertTrue((out / p0 / "_SUCCESS").exists())

    def test_cache_hit_and_factor_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            swap = root / "swap_lin"
            spot = root / "spot_lin"
            out = root / "out"
            swap.mkdir(parents=True)
            spot.mkdir(parents=True)
            self._write_sample_csv(swap / "ETH-USDT.csv")
            p_cfg = preprocess_fast.PreprocessConfig(
                source_swap_dir=swap,
                source_spot_dir=spot,
                output_root=out,
                file_format="pkl",
                compression="zstd",
                include_optional_indicators=False,
            )
            preprocess_fast.preprocess_all(p_cfg)
            l_cfg = factor_loader_mod.FactorLoadConfig(preprocess=p_cfg, use_mmap=False, max_cache_items=32)
            loader = factor_loader_mod.FactorLoader(l_cfg)
            st = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
            et = pd.Timestamp("2026-01-01 03:00:00", tz="UTC")
            r1 = loader.factor_entry(market="swap", symbol="ETH-USDT", start=st, end=et, factor="ma", window=2)
            self.assertFalse(r1.empty)
            h1 = loader.cache_hit
            _ = loader.factor_entry(market="swap", symbol="ETH-USDT", start=st, end=et, factor="ma", window=2)
            self.assertGreater(loader.cache_hit, h1)

    def test_incremental_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "config.yaml"
            bad_output_file = root / "not_dir"
            bad_output_file.write_text("x", encoding="utf-8")
            payload = {
                "preprocess": {
                    "source_swap_dir": str(root / "swap"),
                    "source_spot_dir": str(root / "spot"),
                    "output_root": str(bad_output_file),
                    "file_format": "pkl",
                    "compression": "zstd",
                    "include_optional_indicators": False,
                },
                "monitoring": {
                    "log_file": str(root / "metrics.jsonl"),
                    "alert_file": str(root / "alerts.jsonl"),
                },
            }
            try:
                import yaml

                cfg_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
            except Exception:
                cfg_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            result = inc_mod.run_incremental_once(cfg_path)
            self.assertFalse(result.ok)
            self.assertTrue((root / "alerts.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
