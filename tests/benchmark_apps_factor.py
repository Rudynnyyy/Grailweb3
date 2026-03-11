from __future__ import annotations

import json
import os
import statistics
import tempfile
import time
from datetime import timezone
from pathlib import Path

import pandas as pd

from apps.crypto_screener.app.filter_engine import ema, obv_with_ma, rolling_std, rsi, sma
from apps.crypto_screener.app.series_source import load_symbol_series


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _merge_dirs(repo_root: Path) -> tuple[Path, Path]:
    swap = Path(os.environ.get("QC_MERGE_SWAP_PATH") or str(repo_root / "数据获取" / "data" / "swap_lin"))
    spot = Path(os.environ.get("QC_MERGE_SPOT_PATH") or str(repo_root / "数据获取" / "data" / "spot_lin"))
    return swap, spot


def _load_sample_rows(repo_root: Path, max_rows: int = 120) -> list[tuple[str, str]]:
    swap, spot = _merge_dirs(repo_root)
    out: list[tuple[str, str]] = []
    for p in sorted(swap.glob("*.csv"))[: max_rows // 2]:
        out.append(("swap", p.stem.upper()))
    for p in sorted(spot.glob("*.csv"))[: max_rows - len(out)]:
        out.append(("spot", p.stem.upper()))
    return out


def _build_preprocessed_for_samples(repo_root: Path, out_root: Path, rows: list[tuple[str, str]]) -> int:
    old_flag = os.environ.get("QC_USE_PREPROCESSED_SERIES")
    os.environ["QC_USE_PREPROCESSED_SERIES"] = "0"
    count = 0
    files: list[dict] = []
    for market, symbol in rows:
        s = load_symbol_series(market=market, symbol=symbol, tail=960, repo_root=repo_root)
        if s is None:
            continue
        dt = pd.to_datetime(list(s.dt or []), utc=True, errors="coerce")
        if dt.empty or dt.notna().sum() <= 0:
            continue
        df = pd.DataFrame({"dt": dt})
        for col in ("open", "high", "low", "close", "volume", "quote_volume"):
            arr = (s.series or {}).get(col) or []
            if len(arr) != len(df):
                continue
            df[col] = pd.to_numeric(arr, errors="coerce")
        df = df.dropna(subset=["dt"]).copy()
        if df.empty:
            continue
        last = pd.to_datetime(df["dt"].iloc[-1], utc=True)
        pdir = out_root / market / f"{last.year:04d}" / f"{last.month:02d}" / f"{last.day:02d}" / f"{last.hour:02d}"
        pdir.mkdir(parents=True, exist_ok=True)
        fpath = pdir / f"{symbol}.pkl"
        df.to_pickle(fpath, protocol=5)
        part = f"{market}/{last.year:04d}/{last.month:02d}/{last.day:02d}/{last.hour:02d}"
        rec = {
            "market": market,
            "symbol": symbol,
            "partition": part,
            "file": f"{symbol}.pkl",
            "row_count": int(len(df)),
            "time_range": {"start": pd.to_datetime(df["dt"].iloc[0], utc=True).isoformat(), "end": pd.to_datetime(df["dt"].iloc[-1], utc=True).isoformat()},
            "md5": "",
            "size_bytes": int(fpath.stat().st_size),
        }
        files.append(rec)
        (pdir / "manifest.json").write_text(json.dumps({"version": 1, "partition": part, "files": [rec]}, ensure_ascii=False), encoding="utf-8")
        (pdir / "_SUCCESS").write_text(pd.Timestamp.now(tz=timezone.utc).isoformat(), encoding="utf-8")
        count += 1
    (out_root / "manifest.json").write_text(json.dumps({"version": 1, "files": files}, ensure_ascii=False), encoding="utf-8")
    (out_root / "_SUCCESS").write_text(pd.Timestamp.now(tz=timezone.utc).isoformat(), encoding="utf-8")
    if old_flag is None:
        os.environ.pop("QC_USE_PREPROCESSED_SERIES", None)
    else:
        os.environ["QC_USE_PREPROCESSED_SERIES"] = old_flag
    return count


def _run_case(repo_root: Path, rows: list[tuple[str, str]], *, use_preprocessed: bool, out_root: Path, loops: int = 3) -> dict:
    os.environ["QC_USE_PREPROCESSED_SERIES"] = "1" if use_preprocessed else "0"
    os.environ["QC_PREPROCESS_OUT_ROOT"] = str(out_root)
    costs: list[float] = []
    cpu_costs: list[float] = []
    ok_count = 0
    for _ in range(max(1, loops)):
        t0 = time.perf_counter()
        c0 = time.process_time()
        cnt = 0
        for market, symbol in rows:
            s = load_symbol_series(market=market, symbol=symbol, tail=720, repo_root=repo_root)
            if s is None:
                continue
            close = s.series.get("close") or []
            high = s.series.get("high") or []
            low = s.series.get("low") or []
            vol = s.series.get("volume") or []
            _ = sma(close, 20)
            _ = rsi(close, 14)
            _ = ema(close, 20)
            _ = rolling_std(close, 20)
            _ = obv_with_ma(close, vol, 20)
            _ = ema(high, 10)
            _ = ema(low, 10)
            cnt += 1
        c1 = time.process_time()
        t1 = time.perf_counter()
        costs.append(t1 - t0)
        cpu_costs.append(c1 - c0)
        ok_count = max(ok_count, cnt)
    return {
        "use_preprocessed": use_preprocessed,
        "rows_processed": ok_count,
        "wall_median": statistics.median(costs),
        "cpu_median": statistics.median(cpu_costs),
        "wall_all": costs,
        "cpu_all": cpu_costs,
    }


def _pct_drop(old: float, new: float) -> float:
    if old <= 0:
        return 0.0
    return (old - new) / old * 100.0


def main() -> None:
    repo_root = _repo_root()
    rows = _load_sample_rows(repo_root, max_rows=120)
    if not rows:
        raise SystemExit("无可用样本CSV")
    with tempfile.TemporaryDirectory() as td:
        out_root = Path(td) / "preprocessed_hourly_bench"
        out_root.mkdir(parents=True, exist_ok=True)
        built = _build_preprocessed_for_samples(repo_root, out_root, rows)
        if built <= 0:
            raise SystemExit("样本预处理构建失败")
        baseline = _run_case(repo_root, rows, use_preprocessed=False, out_root=out_root, loops=3)
        optimized = _run_case(repo_root, rows, use_preprocessed=True, out_root=out_root, loops=3)
        wall_drop = _pct_drop(float(baseline["wall_median"]), float(optimized["wall_median"]))
        cpu_drop = _pct_drop(float(baseline["cpu_median"]), float(optimized["cpu_median"]))
        out = {
            "sample_rows": len(rows),
            "sample_built": built,
            "baseline": baseline,
            "optimized": optimized,
            "wall_drop_pct": wall_drop,
            "cpu_drop_pct": cpu_drop,
            "target_pass": {"wall_ge_30": wall_drop >= 30.0, "cpu_ge_20": cpu_drop >= 20.0},
        }
        out_json = repo_root / "docs" / "币圈选股器" / "apps_factor_benchmark.json"
        out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        md = [
            "# apps 因子后端基准",
            "",
            f"- 样本数: {len(rows)}",
            f"- 预处理构建成功: {built}",
            f"- 基线中位耗时(秒): {baseline['wall_median']:.6f}",
            f"- 优化中位耗时(秒): {optimized['wall_median']:.6f}",
            f"- 响应耗时下降: {wall_drop:.2f}%",
            f"- 基线CPU中位(秒): {baseline['cpu_median']:.6f}",
            f"- 优化CPU中位(秒): {optimized['cpu_median']:.6f}",
            f"- CPU占用下降: {cpu_drop:.2f}%",
            f"- 达标(响应≥30%): {wall_drop >= 30.0}",
            f"- 达标(CPU≥20%): {cpu_drop >= 20.0}",
        ]
        out_md = repo_root / "docs" / "币圈选股器" / "benchmark_report_apps_backend.md"
        out_md.write_text("\n".join(md), encoding="utf-8")
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
