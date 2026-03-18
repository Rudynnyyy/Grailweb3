from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


class DefaultDataDirsTests(unittest.TestCase):
    ENV_KEYS = ("QC_MERGE_SWAP_PATH", "QC_MERGE_SPOT_PATH", "QC_SCREENER_FALLBACK_SWAP_DIR", "QC_SCREENER_FALLBACK_SPOT_DIR")

    def setUp(self) -> None:
        self._old = {k: os.environ.get(k) for k in self.ENV_KEYS}
        for k in self.ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self) -> None:
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_series_source_prefers_repo_data_dir_when_exists(self) -> None:
        from apps.crypto_screener.app.series_source import _default_merge_dirs  # noqa: E402

        with tempfile.TemporaryDirectory() as td:
            rr = Path(td)
            (rr / "数据获取" / "data" / "swap_lin").mkdir(parents=True, exist_ok=True)
            (rr / "数据获取" / "data" / "spot_lin").mkdir(parents=True, exist_ok=True)
            swap, spot = _default_merge_dirs(rr)
            self.assertTrue(str(swap).endswith(str(Path("数据获取") / "data" / "swap_lin")))
            self.assertTrue(str(spot).endswith(str(Path("数据获取") / "data" / "spot_lin")))

    def test_series_source_env_overrides(self) -> None:
        from apps.crypto_screener.app.series_source import _default_merge_dirs  # noqa: E402

        with tempfile.TemporaryDirectory() as td:
            rr = Path(td)
            a = rr / "A"
            b = rr / "B"
            a.mkdir(parents=True, exist_ok=True)
            b.mkdir(parents=True, exist_ok=True)
            os.environ["QC_MERGE_SWAP_PATH"] = str(a)
            os.environ["QC_MERGE_SPOT_PATH"] = str(b)
            swap, spot = _default_merge_dirs(rr)
            self.assertEqual(Path(swap).resolve(), a.resolve())
            self.assertEqual(Path(spot).resolve(), b.resolve())


if __name__ == "__main__":
    unittest.main()

