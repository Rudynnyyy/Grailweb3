from __future__ import annotations

import sys
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


class WecomSeriesAttachTests(unittest.TestCase):
    def test_config_needs_series_for_ma_rsi_toggles(self) -> None:
        from apps.crypto_screener.app.web_server import _config_needs_series  # noqa: E402

        self.assertTrue(_config_needs_series({"toggles": {"condCloseMa": True}, "params": {"maPeriodClose": 20}}))
        self.assertTrue(_config_needs_series({"toggles": {"condMa": True}, "params": {"maFast": 10, "maSlow": 20}}))
        self.assertTrue(_config_needs_series({"toggles": {"condRsi": True}, "params": {"rsiPeriod": 14, "rsiThreshold": 80}}))

    def test_config_needs_series_for_custom_factors(self) -> None:
        from apps.crypto_screener.app.web_server import _config_needs_series  # noqa: E402

        cfg = {"customFactors": [{"id": "x", "enabled": True, "template": "close/close.shift(1)-1"}]}
        self.assertTrue(_config_needs_series(cfg))

    def test_config_does_not_need_series_for_empty(self) -> None:
        from apps.crypto_screener.app.web_server import _config_needs_series  # noqa: E402

        self.assertFalse(_config_needs_series({}))


if __name__ == "__main__":
    unittest.main()

