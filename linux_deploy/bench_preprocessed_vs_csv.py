from __future__ import annotations

import argparse
import json
import os
import sys
import statistics
import time
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_rss_kb() -> int:
    p = Path("/proc/self/status")
    try:
        txt = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return 0
    for ln in txt.splitlines():
        if ln.startswith("VmRSS:"):
            parts = ln.split()
            if len(parts) >= 2:
                try:
                    return int(parts[1])
                except Exception:
                    return 0
    return 0


def _sample_symbols(*, base_dir: Path, limit: int) -> list[str]:
    out: list[str] = []
    if not base_dir.exists():
        return out
    for p in sorted(base_dir.glob("*.csv")):
        s = str(p.stem or "").strip()
        if not s:
            continue
        out.append(s.upper())
        if len(out) >= int(limit):
            break
    return out


def _load_factor_payload(*, repo_root: Path, market: str) -> dict:
    import pickle

    out_root = os.environ.get("QC_PREPROCESS_OUT_ROOT") or str(repo_root / "数据获取" / "data" / "preprocessed_hourly")
    root = Path(os.environ.get("QC_PKL_CACHE_ROOT") or (Path(out_root) / "pkl_cache"))
    fp = root / f"factors_{str(market).lower()}.pkl"
    if not fp.exists():
        return {}
    try:
        with fp.open("rb") as f:
            payload = pickle.load(f)
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _run_case(*, repo_root: Path, market: str, symbols: list[str], tail: int, loops: int, use_preprocessed: bool, use_factor_cache: bool) -> dict:
    os.environ["QC_USE_PREPROCESSED_SERIES"] = "1" if use_preprocessed else "0"
    from importlib import import_module, reload

    series_source = reload(import_module("apps.crypto_screener.app.series_source"))
    filter_engine = import_module("apps.crypto_screener.app.filter_engine")
    factors_payload = _load_factor_payload(repo_root=repo_root, market=market) if use_factor_cache else {}
    factors_map = factors_payload.get("factors") if isinstance(factors_payload.get("factors"), dict) else {}

    wall_costs: list[float] = []
    cpu_costs: list[float] = []
    rss_peaks: list[int] = []
    processed = 0
    for _ in range(max(1, int(loops))):
        rss0 = _read_rss_kb()
        rss_peak = rss0
        t0 = time.perf_counter()
        c0 = time.process_time()
        cnt = 0
        for sym in symbols:
            if use_factor_cache:
                rec = factors_map.get(str(sym).upper())
                if not isinstance(rec, dict):
                    continue
                _ = rec.get("ma_20")
                _ = rec.get("rsi_14")
                _ = rec.get("ema_20")
                _ = rec.get("std_20")
                _ = rec.get("obv")
                _ = rec.get("obv_ma")
                _ = rec.get("supertrend")
            else:
                s = series_source.load_symbol_series(market=market, symbol=sym, tail=int(tail), repo_root=repo_root)
                if s is None:
                    continue
                close = (s.series or {}).get("close") or []
                high = (s.series or {}).get("high") or []
                low = (s.series or {}).get("low") or []
                vol = (s.series or {}).get("volume") or []
                _ = filter_engine.sma(close, 20)
                _ = filter_engine.rsi(close, 14)
                _ = filter_engine.ema(close, 20)
                _ = filter_engine.rolling_std(close, 20)
                _ = filter_engine.obv_with_ma(close, vol, 20)
                _ = filter_engine.ema(high, 10)
                _ = filter_engine.ema(low, 10)
            cnt += 1
            rss_now = _read_rss_kb()
            if rss_now > rss_peak:
                rss_peak = rss_now
        c1 = time.process_time()
        t1 = time.perf_counter()
        wall_costs.append(t1 - t0)
        cpu_costs.append(c1 - c0)
        rss_peaks.append(rss_peak)
        processed = max(processed, cnt)
    wall_med = statistics.median(wall_costs) if wall_costs else 0.0
    cpu_med = statistics.median(cpu_costs) if cpu_costs else 0.0
    rss_peak_med = int(statistics.median(rss_peaks)) if rss_peaks else 0
    cpu_util = (cpu_med / wall_med * 100.0) if wall_med > 0 else 0.0
    return {
        "market": market,
        "use_preprocessed": bool(use_preprocessed),
        "use_factor_cache": bool(use_factor_cache),
        "symbols_requested": len(symbols),
        "symbols_processed": int(processed),
        "tail": int(tail),
        "loops": int(loops),
        "wall_median_s": float(wall_med),
        "cpu_median_s": float(cpu_med),
        "cpu_util_pct_est": float(round(cpu_util, 2)),
        "rss_peak_median_kb": int(rss_peak_med),
        "wall_all_s": wall_costs,
        "cpu_all_s": cpu_costs,
        "rss_peak_all_kb": rss_peaks,
    }


def _pct_drop(old: float, new: float) -> float:
    if old <= 0:
        return 0.0
    return (old - new) / old * 100.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", type=str, default="swap", choices=["swap", "spot"])
    parser.add_argument("--tail", type=int, default=360)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--loops", type=int, default=5)
    parser.add_argument("--use-factor-cache", action="store_true")
    parser.add_argument("--out-json", type=str, default="")
    args = parser.parse_args()

    repo_root = _repo_root()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    base = Path(os.environ.get("QC_MERGE_SWAP_PATH") or (repo_root / "数据获取" / "data" / "swap_lin"))
    if str(args.market).lower() == "spot":
        base = Path(os.environ.get("QC_MERGE_SPOT_PATH") or (repo_root / "数据获取" / "data" / "spot_lin"))

    syms = _sample_symbols(base_dir=base, limit=int(args.samples))
    if not syms:
        raise SystemExit(f"no_sample_csv base_dir={base}")

    out_root = os.environ.get("QC_PREPROCESS_OUT_ROOT") or str(repo_root / "数据获取" / "data" / "preprocessed_hourly")
    os.environ.setdefault("QC_PREPROCESS_OUT_ROOT", out_root)

    baseline = _run_case(
        repo_root=repo_root,
        market=str(args.market).lower(),
        symbols=syms,
        tail=int(args.tail),
        loops=int(args.loops),
        use_preprocessed=False,
        use_factor_cache=False,
    )
    optimized = _run_case(
        repo_root=repo_root,
        market=str(args.market).lower(),
        symbols=syms,
        tail=int(args.tail),
        loops=int(args.loops),
        use_preprocessed=True,
        use_factor_cache=False,
    )
    factor_cached = None
    if bool(args.use_factor_cache):
        factor_cached = _run_case(
            repo_root=repo_root,
            market=str(args.market).lower(),
            symbols=syms,
            tail=int(args.tail),
            loops=int(args.loops),
            use_preprocessed=False,
            use_factor_cache=True,
        )
    wall_drop = _pct_drop(float(baseline["wall_median_s"]), float(optimized["wall_median_s"]))
    cpu_drop = _pct_drop(float(baseline["cpu_median_s"]), float(optimized["cpu_median_s"]))
    mem_drop = _pct_drop(float(baseline["rss_peak_median_kb"]), float(optimized["rss_peak_median_kb"]))

    report = {
        "env": {
            "QC_PREPROCESS_OUT_ROOT": os.environ.get("QC_PREPROCESS_OUT_ROOT"),
            "QC_MERGE_SWAP_PATH": os.environ.get("QC_MERGE_SWAP_PATH"),
            "QC_MERGE_SPOT_PATH": os.environ.get("QC_MERGE_SPOT_PATH"),
        },
        "market": str(args.market).lower(),
        "tail": int(args.tail),
        "samples": int(args.samples),
        "loops": int(args.loops),
        "baseline": baseline,
        "preprocessed": optimized,
        "factor_cache": factor_cached,
        "drop_pct": {"wall": float(round(wall_drop, 4)), "cpu": float(round(cpu_drop, 4)), "rss_peak": float(round(mem_drop, 4))},
    }

    out_json = str(args.out_json or "").strip()
    if out_json:
        Path(out_json).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
