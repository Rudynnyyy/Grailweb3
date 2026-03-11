from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


inc = importlib.import_module("数据获取.incremental_update")


class IncrementalLagTests(unittest.TestCase):
    def test_infer_latest_preprocessed_hour(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mf = root / "manifest.json"
            payload = {
                "version": 1,
                "files": [
                    {"time_range": {"start": "2026-03-06T06:00:00+00:00", "end": "2026-03-06T06:00:00+00:00"}},
                    {"time_range": {"start": "2026-03-06T07:00:00+00:00", "end": "2026-03-06T07:00:00+00:00"}},
                ],
            }
            mf.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            ts = inc.infer_latest_preprocessed_hour(root)
            self.assertEqual(ts, pd.Timestamp("2026-03-06 07:00:00", tz="UTC"))

    def test_compute_catchup_window(self) -> None:
        now = datetime(2026, 3, 6, 9, 30, tzinfo=timezone.utc)
        latest = pd.Timestamp("2026-03-06 07:00:00", tz="UTC")
        st, et, hours = inc._compute_catchup_window(latest_done=latest, now_utc=now, lag_hours=1, max_hours=24)
        self.assertEqual(st, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(et, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(hours, 1)
        st2, et2, h2 = inc._compute_catchup_window(latest_done=latest, now_utc=now, lag_hours=0, max_hours=24)
        self.assertEqual(st2, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(et2, pd.Timestamp("2026-03-06 09:00:00", tz="UTC"))
        self.assertEqual(h2, 2)
        src = pd.Timestamp("2026-03-06 08:00:00", tz="UTC")
        st3, et3, h3 = inc._compute_catchup_window(latest_done=latest, source_latest=src, now_utc=now, lag_hours=0, max_hours=24)
        self.assertEqual(st3, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(et3, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(h3, 1)
        st4, et4, h4 = inc._compute_catchup_window(latest_done=None, source_latest=src, now_utc=now, lag_hours=0, max_hours=4)
        self.assertEqual(et4, pd.Timestamp("2026-03-06 08:00:00", tz="UTC"))
        self.assertEqual(h4, 4)
        self.assertEqual(st4, pd.Timestamp("2026-03-06 05:00:00", tz="UTC"))


if __name__ == "__main__":
    unittest.main()
