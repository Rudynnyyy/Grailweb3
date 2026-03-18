from __future__ import annotations

import os
import tempfile
from pathlib import Path
import sys


def _write_csv(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        "\n".join(
            [
                "candle_begin_time,open,high,low,close,volume,quote_volume",
                "2026-01-01 00:00:00,1,1,1,1,10,10",
                "2026-01-01 01:00:00,2,2,2,2,10,10",
                "2026-01-01 02:00:00,3,3,3,3,10,10",
                "2026-01-01 03:00:00,4,4,4,4,10,10",
                "2026-01-01 04:00:00,5,5,5,5,10,10",
                "2026-01-01 05:00:00,6,6,6,6,10,10",
                "2026-01-01 06:00:00,7,7,7,7,10,10",
                "2026-01-01 07:00:00,8,8,8,8,10,10",
                "2026-01-01 08:00:00,9,9,9,9,10,10",
                "2026-01-01 09:00:00,10,10,10,10,10,10",
                "2026-01-01 10:00:00,11,11,11,11,10,10",
                "2026-01-01 11:00:00,12,12,12,12,10,10",
                "2026-01-01 12:00:00,13,13,13,13,10,10",
                "2026-01-01 13:00:00,14,14,14,14,10,10",
                "2026-01-01 14:00:00,15,15,15,15,10,10",
                "2026-01-01 15:00:00,16,16,16,16,10,10",
                "2026-01-01 16:00:00,17,17,17,17,10,10",
                "2026-01-01 17:00:00,18,18,18,18,10,10",
                "2026-01-01 18:00:00,19,19,19,19,10,10",
                "2026-01-01 19:00:00,20,20,20,20,10,10",
                "2026-01-01 20:00:00,21,21,21,21,10,10",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    with tempfile.TemporaryDirectory() as td:
        td0 = Path(td)
        merge_swap = td0 / "swap_lin"
        dc_root = td0 / "data_center" / "kline" / "swap"
        sym = "BTC-USDT"
        _write_csv(merge_swap / f"{sym}.csv")
        _write_csv(dc_root / f"{sym}.csv")

        os.environ["QC_MERGE_SWAP_PATH"] = str(merge_swap)
        os.environ["QC_DATA_CENTER_ROOT"] = str(td0 / "data_center")
        os.environ["QC_PREPROCESS_OUT_ROOT"] = str(td0 / "preprocessed_hourly")
        os.environ["QC_USE_PKL_SERIES_CACHE"] = "1"

        from 数据获取.factor_cache_update import build_market_cache

        build_market_cache(market="swap", tail=40, symbols_limit=0, workers=2, incremental=False)

        from apps.crypto_screener.app.series_source import load_symbol_series

        s = load_symbol_series(market="swap", symbol=sym, tail=30, repo_root=repo_root)
        if s is None or s.source != "pkl_cache":
            raise SystemExit("series_cache_failed")

        from apps.crypto_screener.app.filter_engine import compute_builtins

        row = {"market": "swap", "symbol": sym, "series": s.series, "close": s.series.get("close", [None])[-1]}
        b = compute_builtins(row, {"maPeriodClose": 20, "maFast": 10, "maSlow": 20, "rsiPeriod": 14, "market": "swap"})
        if not isinstance(b, dict) or "ma_20" not in b or "rsi_14" not in b:
            raise SystemExit("factor_cache_failed")


if __name__ == "__main__":
    main()
