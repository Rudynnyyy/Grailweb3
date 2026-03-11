from __future__ import annotations

import importlib
import time
import unittest

import numpy as np
import pandas as pd


factor_loader_mod = importlib.import_module("数据获取.factor_loader")


class FactorPerfTests(unittest.TestCase):
    def test_vectorized_speedup(self) -> None:
        n = 220000
        close = np.linspace(10, 120, n) + np.sin(np.arange(n) / 30.0)
        df = pd.DataFrame({"close": close})
        t0 = time.perf_counter()
        v = factor_loader_mod.compute_factor_vectorized(df, factor="ma_bias", window=20)
        t1 = time.perf_counter()
        l = factor_loader_mod.compute_factor_loop(df, factor="ma_bias", window=20)
        t2 = time.perf_counter()
        vec_cost = t1 - t0
        loop_cost = t2 - t1
        speedup = loop_cost / max(vec_cost, 1e-9)
        self.assertGreaterEqual(speedup, 5.0)
        self.assertEqual(len(v), len(l))

    def test_factor_value_consistency(self) -> None:
        n = 20000
        close = np.random.default_rng(7).normal(100, 8, size=n).clip(1, None)
        df = pd.DataFrame({"close": close})
        v = factor_loader_mod.compute_factor_vectorized(df, factor="ret1", window=20)
        l = factor_loader_mod.compute_factor_loop(df, factor="ret1", window=20)
        diff = np.nanmean(np.abs(v.values - l.values))
        self.assertLess(diff, 1e-12)


if __name__ == "__main__":
    unittest.main()
