from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import pandas as pd


series_source = importlib.import_module("apps.crypto_screener.app.series_source")
web_server = importlib.import_module("apps.crypto_screener.app.web_server")


class AppsPreprocessedSeriesTests(unittest.TestCase):
    ENV_KEYS = (
        "QC_USE_PREPROCESSED_SERIES",
        "QC_PREPROCESS_OUT_ROOT",
        "QC_MERGE_SWAP_PATH",
        "QC_MERGE_SPOT_PATH",
        "QC_SCREENER_FALLBACK_SWAP_DIR",
        "QC_SCREENER_FALLBACK_SPOT_DIR",
    )

    def setUp(self) -> None:
        self._env_old = {k: os.environ.get(k) for k in self.ENV_KEYS}

    def tearDown(self) -> None:
        for k, v in self._env_old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        with web_server.series_cache_lock:
            web_server.series_cache.clear()

    def _write_merge_csv(self, path: Path) -> None:
        df = pd.DataFrame(
            {
                "candle_begin_time": ["2026-01-01 00:00:00", "2026-01-01 01:00:00", "2026-01-01 02:00:00"],
                "open": [1.0, 2.0, 3.0],
                "high": [1.1, 2.1, 3.1],
                "low": [0.9, 1.9, 2.9],
                "close": [1.05, 2.05, 3.05],
                "volume": [10, 20, 30],
                "quote_volume": [11, 22, 33],
            }
        )
        df.to_csv(path, index=False, encoding="utf-8")

    def _write_preprocessed_symbol(self, root: Path, market: str, symbol: str) -> None:
        files = []
        for h in (0, 1, 2):
            pdir = root / market / "2026" / "01" / "01" / f"{h:02d}"
            pdir.mkdir(parents=True, exist_ok=True)
            fpath = pdir / f"{symbol}.pkl"
            df = pd.DataFrame(
                {
                    "dt": [pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC")],
                    "open": [1.0 + h],
                    "high": [1.1 + h],
                    "low": [0.9 + h],
                    "close": [1.05 + h],
                    "volume": [10.0 + h],
                    "quote_volume": [11.0 + h],
                }
            )
            df.to_pickle(fpath, protocol=5)
            files.append(
                {
                    "market": market,
                    "symbol": symbol,
                    "partition": f"{market}/2026/01/01/{h:02d}",
                    "file": f"{symbol}.pkl",
                    "row_count": 1,
                    "time_range": {
                        "start": pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC").isoformat(),
                        "end": pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC").isoformat(),
                    },
                    "md5": "x",
                    "size_bytes": int(fpath.stat().st_size),
                }
            )
            (pdir / "_SUCCESS").write_text("ok", encoding="utf-8")
            (pdir / "manifest.json").write_text(json.dumps({"version": 1, "files": [files[-1]]}, ensure_ascii=False), encoding="utf-8")
        g = {"version": 1, "files": files}
        (root / "manifest.json").write_text(json.dumps(g, ensure_ascii=False), encoding="utf-8")
        (root / "_SUCCESS").write_text("ok", encoding="utf-8")

    def test_preprocessed_series_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            out_root = t / "preprocessed"
            self._write_preprocessed_symbol(out_root, "swap", "BTC-USDT")
            swap_dir = t / "swap_lin"
            spot_dir = t / "spot_lin"
            swap_dir.mkdir()
            spot_dir.mkdir()
            self._write_merge_csv(swap_dir / "BTC-USDT.csv")
            os.environ["QC_USE_PREPROCESSED_SERIES"] = "1"
            os.environ["QC_PREPROCESS_OUT_ROOT"] = str(out_root)
            os.environ["QC_MERGE_SWAP_PATH"] = str(swap_dir)
            os.environ["QC_MERGE_SPOT_PATH"] = str(spot_dir)
            s = series_source.load_symbol_series(market="swap", symbol="BTC-USDT", tail=3, repo_root=t)
            self.assertIsNotNone(s)
            self.assertEqual(s.source, "preprocessed")
            self.assertEqual(len(s.dt), 3)

    def test_csv_path_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            out_root = t / "preprocessed"
            self._write_preprocessed_symbol(out_root, "swap", "BTC-USDT")
            swap_dir = t / "swap_lin"
            spot_dir = t / "spot_lin"
            swap_dir.mkdir()
            spot_dir.mkdir()
            self._write_merge_csv(swap_dir / "BTC-USDT.csv")
            os.environ["QC_USE_PREPROCESSED_SERIES"] = "0"
            os.environ["QC_PREPROCESS_OUT_ROOT"] = str(out_root)
            os.environ["QC_MERGE_SWAP_PATH"] = str(swap_dir)
            os.environ["QC_MERGE_SPOT_PATH"] = str(spot_dir)
            s = series_source.load_symbol_series(market="swap", symbol="BTC-USDT", tail=3, repo_root=t)
            self.assertIsNotNone(s)
            self.assertEqual(s.source, "csv")
            self.assertGreaterEqual(len(s.dt), 3)

    def test_web_server_metrics_collect_series_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            out_root = t / "preprocessed"
            self._write_preprocessed_symbol(out_root, "swap", "BTC-USDT")
            swap_dir = t / "swap_lin"
            spot_dir = t / "spot_lin"
            swap_dir.mkdir()
            spot_dir.mkdir()
            self._write_merge_csv(swap_dir / "BTC-USDT.csv")
            os.environ["QC_USE_PREPROCESSED_SERIES"] = "1"
            os.environ["QC_PREPROCESS_OUT_ROOT"] = str(out_root)
            os.environ["QC_MERGE_SWAP_PATH"] = str(swap_dir)
            os.environ["QC_MERGE_SPOT_PATH"] = str(spot_dir)
            with web_server.series_cache_lock:
                web_server.series_cache.clear()
            with web_server.metrics_lock:
                web_server.metrics["series_source"] = {}
            ctx = web_server._get_series_cached(market="swap", symbol="BTC-USDT", tail=3)
            self.assertIsNotNone(ctx)
            self.assertEqual(str(ctx.get("source")), "preprocessed")
            with web_server.metrics_lock:
                ss = web_server.metrics.get("series_source") or {}
            self.assertGreaterEqual(int(ss.get("preprocessed") or 0), 1)

    def test_series_source_private_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            d1 = t / "a"
            d2 = t / "b"
            d1.mkdir()
            d2.mkdir()
            self._write_merge_csv(d1 / "BTC-USDT.csv")
            self._write_merge_csv(d2 / "BTCUSDT.csv")
            os.environ["QC_MERGE_SWAP_PATH"] = str(d1)
            os.environ["QC_SCREENER_FALLBACK_SWAP_DIR"] = str(d2)
            cands = series_source._symbol_candidates("btc-usdt")
            self.assertIn("BTCUSDT", [x.upper() for x in cands])
            p, sym = series_source._pick_existing_csv([d1, d2], "BTC-USDT", tail_hint=100)
            self.assertIsNotNone(p)
            self.assertTrue(str(sym).upper() in ("BTC-USDT", "BTCUSDT"))
            dt0, n0 = series_source._probe_csv_tail_last_dt(p, tail_hint=60)
            self.assertTrue(n0 >= 1)
            self.assertIsNotNone(dt0)
            header = series_source._read_header_line(p)
            self.assertIn("candle_begin_time", header)
            tails = series_source._read_tail_text(p, max_lines=5)
            self.assertTrue(len(tails) >= 1)
            df = series_source.read_merge_csv_tail(p, tail=2, extra=2)
            self.assertEqual(len(df), 2)

    def test_manifest_index_and_preprocessed_toggle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            out_root = t / "preprocessed"
            self._write_preprocessed_symbol(out_root, "swap", "BTC-USDT")
            os.environ["QC_PREPROCESS_OUT_ROOT"] = str(out_root)
            os.environ["QC_USE_PREPROCESSED_SERIES"] = "1"
            self.assertTrue(series_source._preprocessed_enabled())
            self.assertEqual(series_source._preprocessed_root(t), out_root)
            idx = series_source._load_manifest_index(out_root)
            self.assertTrue(("swap", "BTC-USDT") in idx)
            idx2 = series_source._load_manifest_index(out_root)
            self.assertEqual(len(idx), len(idx2))
            bad_root = t / "bad"
            bad_root.mkdir()
            (bad_root / "manifest.json").write_text("{bad json", encoding="utf-8")
            self.assertEqual(series_source._load_manifest_index(bad_root), {})
            self.assertIsNone(series_source._load_series_from_preprocessed(market="swap", symbol="NOPE-USDT", tail=10, repo_root=t))
            txt = out_root / "x.txt"
            txt.write_text("x", encoding="utf-8")
            self.assertTrue(series_source._read_preprocessed_file(txt, columns=["dt"]).empty)

    def test_more_branches_for_series_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            t = Path(td)
            csv_path = t / "num.csv"
            csv_path.write_text(
                "candle_begin_time,open,high,low,close,volume,quote_volume\n"
                "1700000000,1,1,1,1,1,1\n"
                "1700003600,2,2,2,2,2,2\n",
                encoding="utf-8",
            )
            df = series_source.read_merge_csv_tail(csv_path, tail=2, extra=1)
            self.assertEqual(len(df), 2)
            self.assertFalse(series_source._read_tail_text(t / "missing.csv", max_lines=5))
            self.assertEqual(series_source._read_header_line(t / "missing.csv"), "")
            pq_path = t / "x.parquet"
            pq_path.write_text("x", encoding="utf-8")
            with mock.patch.object(series_source.pd, "read_parquet", return_value=pd.DataFrame({"dt": [1]})):
                out = series_source._read_preprocessed_file(pq_path, columns=["dt"])
                self.assertFalse(out.empty)
            with mock.patch.object(series_source, "_load_series_from_preprocessed", side_effect=RuntimeError("x")):
                with mock.patch.object(series_source, "_pick_existing_csv", return_value=(None, "")):
                    s = series_source.load_symbol_series(market="swap", symbol="NOPE-USDT", tail=10, repo_root=t)
                    self.assertIsNone(s)
            with mock.patch.object(series_source, "_pick_existing_csv", return_value=(csv_path, "NOPE-USDT")):
                with mock.patch.object(series_source, "read_merge_csv_tail", return_value=pd.DataFrame()):
                    s2 = series_source.load_symbol_series(market="swap", symbol="NOPE-USDT", tail=10, repo_root=t)
                    self.assertIsNone(s2)


if __name__ == "__main__":
    unittest.main()
