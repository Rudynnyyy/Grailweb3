from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


_REPO_ROOT = _repo_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _sample_symbols(*, base_dir: Path, limit: int) -> list[str]:
    out: list[str] = []
    if not base_dir.exists():
        return out
    for p in sorted(base_dir.glob("*.csv")):
        s = str(p.stem or "").strip().upper()
        if not s:
            continue
        out.append(s)
        if limit > 0 and len(out) >= limit:
            break
    return out


def _last_close(series: dict) -> float | None:
    arr = (series or {}).get("close") or []
    for i in range(len(arr) - 1, -1, -1):
        v = arr[i]
        try:
            x = float(v)
        except Exception:
            continue
        if x == x:
            return x
    return None


def _run_mode(*, repo_root: Path, market: str, symbols: list[str], tail: int, loops: int, mode: str) -> dict:
    mode0 = str(mode or "").strip().lower()
    if mode0 == "baseline":
        os.environ["QC_USE_PKL_SERIES_CACHE"] = "0"
        os.environ["QC_USE_PREPROCESSED_SERIES"] = "0"
    elif mode0 == "preprocessed":
        os.environ["QC_USE_PKL_SERIES_CACHE"] = "0"
        os.environ["QC_USE_PREPROCESSED_SERIES"] = "1"
    elif mode0 in ("pkl_series", "pkl"):
        os.environ["QC_USE_PKL_SERIES_CACHE"] = "1"
        os.environ["QC_USE_PREPROCESSED_SERIES"] = "0"
    elif mode0 in ("pkl_series_factor", "pkl_factor", "both"):
        os.environ["QC_USE_PKL_SERIES_CACHE"] = "1"
        os.environ["QC_USE_PREPROCESSED_SERIES"] = "0"
    else:
        raise ValueError("bad mode")

    from importlib import import_module, reload

    ss = reload(import_module("apps.crypto_screener.app.series_source"))
    fe = reload(import_module("apps.crypto_screener.app.filter_engine"))
    try:
        fe.reset_factor_cache_metrics()
    except Exception:
        pass

    custom_factors = [
        {
            "id": "qv_bias",
            "expr": "quotevolume/quotevolume.ma(n1)",
            "template": "quotevolume/quotevolume.ma(n1)",
            "params": [20],
            "enabled": True,
            "thresholdEnabled": False,
            "showColumn": True,
        }
    ]
    config = {
        "params": {
            "market": market,
            "maPeriodClose": 20,
            "maFast": 10,
            "maSlow": 20,
            "rsiPeriod": 14,
            "rsiThreshold": 60,
            "emaPeriod": 20,
            "bollPeriod": 20,
            "bollStd": 2.0,
            "bollDownPeriod": 20,
            "bollDownStd": 2.0,
            "superAtrPeriod": 10,
            "superMult": 3.0,
            "kdjN": 9,
            "kdjM1": 3,
            "kdjM2": 3,
            "obvMaPeriod": 20,
            "stochRsiP": 14,
            "stochRsiK": 14,
            "stochRsiSmK": 3,
            "stochRsiSmD": 3,
        },
        "toggles": {
            "condCloseMa": True,
            "condMa": True,
            "condRsi": True,
            "condEma": True,
            "condBollUp": True,
            "condBollDown": True,
            "condSuper": True,
            "condKdj": True,
            "condObv": True,
            "condStochRsi": True,
        },
        "customFactors": custom_factors,
        "lists": {"whitelist": [], "blacklist": []},
        "sort": {"key": "close", "order": "desc"},
    }

    wall_all: list[float] = []
    cpu_all: list[float] = []
    processed_max = 0
    selected_max = 0
    for _ in range(max(1, int(loops))):
        t0 = time.perf_counter()
        c0 = time.process_time()
        rows = []
        ok = 0
        for sym in symbols:
            s = ss.load_symbol_series(market=market, symbol=sym, tail=int(tail), repo_root=repo_root)
            if s is None:
                continue
            series = getattr(s, "series", None) or {}
            last_close = _last_close(series)
            rows.append({"market": market, "symbol": sym, "series": series, "close": last_close})
            ok += 1
        processed_max = max(processed_max, ok)
        res = fe.apply_all_filters(rows, config)
        sel = res.get("selected") if isinstance(res, dict) else []
        selected_max = max(selected_max, len(sel) if isinstance(sel, list) else 0)
        c1 = time.process_time()
        t1 = time.perf_counter()
        wall_all.append(t1 - t0)
        cpu_all.append(c1 - c0)

    wall_med = sorted(wall_all)[len(wall_all) // 2]
    cpu_med = sorted(cpu_all)[len(cpu_all) // 2]
    out = {
        "mode": mode0,
        "market": market,
        "tail": int(tail),
        "loops": int(loops),
        "symbols_requested": len(symbols),
        "symbols_processed": int(processed_max),
        "selected_max": int(selected_max),
        "wall_median_s": float(wall_med),
        "cpu_median_s": float(cpu_med),
        "wall_all_s": wall_all,
        "cpu_all_s": cpu_all,
    }
    try:
        out["factor_cache_metrics"] = fe.get_factor_cache_metrics()
    except Exception:
        out["factor_cache_metrics"] = None
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", type=str, default="swap", choices=["swap", "spot"])
    parser.add_argument("--tail", type=int, default=720)
    parser.add_argument("--samples", type=int, default=300)
    parser.add_argument("--loops", type=int, default=3)
    parser.add_argument("--modes", type=str, default="baseline,preprocessed,pkl_series,pkl_series_factor")
    parser.add_argument("--out-json", type=str, default="")
    args = parser.parse_args()

    repo_root = _repo_root()
    market = str(args.market).lower()
    if market == "swap":
        base_dir = Path(os.environ.get("QC_MERGE_SWAP_PATH") or str(repo_root / "数据获取" / "data" / "swap_lin"))
    else:
        base_dir = Path(os.environ.get("QC_MERGE_SPOT_PATH") or str(repo_root / "数据获取" / "data" / "spot_lin"))
    syms = _sample_symbols(base_dir=base_dir, limit=int(args.samples))

    modes = [m.strip() for m in str(args.modes or "").split(",") if m.strip()]
    results = []
    for m in modes:
        results.append(_run_mode(repo_root=repo_root, market=market, symbols=syms, tail=int(args.tail), loops=int(args.loops), mode=m))

    out = {
        "env": {
            "QC_PREPROCESS_OUT_ROOT": os.environ.get("QC_PREPROCESS_OUT_ROOT"),
            "QC_PKL_CACHE_ROOT": os.environ.get("QC_PKL_CACHE_ROOT"),
            "QC_USE_PKL_SERIES_CACHE": os.environ.get("QC_USE_PKL_SERIES_CACHE"),
            "QC_USE_PREPROCESSED_SERIES": os.environ.get("QC_USE_PREPROCESSED_SERIES"),
        },
        "results": results,
    }
    text = json.dumps(out, ensure_ascii=False)
    if str(args.out_json or "").strip():
        Path(str(args.out_json)).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
